"""
A collection of classes that implement persistence

Currently, this class is only designed to track broadcast items, like markers,
atom locations, and map drawings. A brief description of some event types
follows.

Atoms:
  a-f-G-U-C / User Update (f-G-U-C) points
  a-u-G / Marker

Bits:
  b-m-p-w-GOTO # Go to this thing
  b-m-p-s-p-i # Digital Pointer (cuepoint)
  b-m-p (Generic point prefix, log all)
  b-f-t-r / Picture / File download request (Don't log)
  b-r-f-h-c / EVAC
  b-t-f / GeoChat
  b-a-o-tbl / Emergency
  b-a-o-can / Emergency canceled

User (?) Drawings
  u-d-c / Drawing (circle)
  u-d-r / Drawing (rectangle)
  u-d-f / Drawing (line / polygon)

UDP Like Commands
  t-x-c-t / Ping
  c - Capability
  r - Reply
"""

from abc import ABC, abstractmethod
from datetime import datetime as dt
import logging
import io
import hashlib
from lxml import etree

try:
    import redis  # Only required to instantiate RedisPersistence backend
except ImportError:
    redis = None

try:
    import oci  # Only required to instantiate OraclePersistence backend
except ImportError:
    oci = None

from taky.config import app_config as config
from . import models

KEPT_EVENTS = [
    "a-",
    "b-m-p",
    "b-r-f-h-c",
    "u-d-c",
    "u-d-r",
    "u-d-f",
]


def build_persistence():
    """
    Factory method to build a Persistence object from the given config
    """
    if config.get("taky", "persistence") == "redis":
        if config.get("redis", "server")[:8] == 'redis://':
            return RedisPersistence(
                keyspace=config.get("taky", "hostname"), 
                conn_str=config.get("redis", "server"))
        else:
            return RedisPersistence()  # assume localhost & no keyspace if no conn string provided
    if config.get("taky", "persistence") == "oracle":
        return OraclePersistence(taky_config=config)
    else:
        return Persistence()


class BasePersistence(ABC):
    def __init__(self):
        self.lgr = logging.getLogger(self.__class__.__name__)

    def track(self, event):
        """
        @return False if the item should not be tracked, otherwise the TTL
        """
        ttl = False
        # TODO: Regex probably faster?
        for etype in KEPT_EVENTS:
            if event.etype.startswith(etype):
                ttl = event.persist_ttl
                break

        if not ttl or ttl < 0:
            return

        if self.event_exists(event.uid):
            self.lgr.debug("Updating tracking for: %s (ttl: %d)", event, ttl)
        else:
            self.lgr.debug("New item to track: %s (ttl: %d)", event, ttl)

        self.track_event(event, ttl)

    @abstractmethod
    def track_event(self, event, ttl):
        """
        Add the event to the database
        """
        raise NotImplementedError()

    @abstractmethod
    def get_all(self):
        """
        Return all items tracked
        """
        raise NotImplementedError()

    @abstractmethod
    def get_event(self, uid):
        """
        Return a specific Event by UID. Returns None if the event does not
        exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def event_exists(self, uid):
        """
        Return true if the event exists
        """
        raise NotImplementedError()

    @abstractmethod
    def prune(self):
        """
        Prune the collection
        """
        # In this case, assume nothing needs to be done
        return


class Persistence(BasePersistence):
    """
    A simple memory based persistence object. Events are stored as objects in a
    dictionary. Whenever the dictionary is updated or accessed, it is pruned.

    This object has no long term storage. If taky quits, all the objects are
    lost.
    """

    def __init__(self):
        super().__init__()
        self.events = {}

    def track_event(self, event, ttl):
        self.events[event.uid] = event
        self.prune()

    def event_exists(self, uid):
        return uid in self.events

    def get_event(self, uid):
        self.prune()
        return self.events.get("uid")

    def get_all(self):
        self.prune()
        ret = self.events.values()
        return ret

    def prune(self):
        """
        Go through the database, and delete items that have expired
        """
        uids = []
        now = dt.utcnow()

        for item in self.events.values():
            if now > item.stale:
                self.lgr.info("Pruning %s, stale is %s", item, item.stale)
                uids.append(item.uid)

        for uid in uids:
            self.events.pop(uid)


class RedisPersistence(BasePersistence):
    """
    A Redis backed persistence object, useful for keeping track of events,
    even if taky restarts. This also allows other systems which can
    communicate with Redis to access the events.

    Events are stored as raw XML, and makes use of Redis' EXPIRE command to
    automatically prune events.

    The events are stored under the following keyspace:
      taky:{keyspace}:persist:{event.uid} = <xml>

    In most configurations, keyspace should be the hostname.
    """
    def __init__(self, keyspace=None, conn_str=None):
        super().__init__()

        # Require Redis module:
        if redis is None:
            raise ImportError("Redis module is not installed. Please install 'redis' package.")
        
        self.rds_ok = True

        if keyspace:
            self.rds_ks = f"taky:{keyspace}:persist"
        else:
            self.rds_ks = "taky:persist"

        if conn_str:
            self.lgr.info("Connecting to %s", conn_str)
            self.rds = redis.StrictRedis.from_url(conn_str)
        else:
            self.lgr.info("Connecting to default redis")
            self.rds = redis.StrictRedis()

        try:
            total = len(self.rds.keys(f"{self.rds_ks}:*"))
            self.lgr.info("Tracking %d items", total)
            self._redis_result(True)
        except redis.ConnectionError:
            self._redis_result(False)
    
    def prune(self):
        """ Redis backend does not require pruning. """
        return

    def _redis_result(self, result):
        """
        Simple set/reset latch to notify the user if the connection to the
        redis server is lost
        """
        if self.rds_ok and not result:
            self.lgr.warning("Lost connection to redis")
        elif not self.rds_ok and result:
            self.lgr.warning("Connection to redis restored")

        self.rds_ok = result

    def track_event(self, event, ttl):
        try:
            key = f"{self.rds_ks}:{event.uid}"
            self.rds.set(key, etree.tostring(event.as_element))
            self.rds.expire(key, ttl)
            self._redis_result(True)
        except redis.ConnectionError:
            self._redis_result(False)

    def event_exists(self, uid):
        return self._event_exists(uid)

    def _event_exists(self, uid, uid_is_redis_key=False):
        if uid_is_redis_key:
            key = uid
        else:
            key = f"{self.rds_ks}:{uid}"

        exists = False

        try:
            exists = self.rds.exists(key) > 0
            self._redis_result(True)
        except redis.ConnectionError:
            self._redis_result(False)

        return exists

    def get_event(self, uid):
        return self._get_event(uid)

    def _get_event(self, uid, uid_is_redis_key=False):
        if uid_is_redis_key:
            key = uid
        else:
            key = f"{self.rds_ks}:{uid}"

        evt = None
        purge = False
        try:
            xml = self.rds.get(key)
            self._redis_result(True)
            if xml is None:
                return None

            parser = etree.XMLParser(resolve_entities=False)
            parser.feed(xml)
            elm = parser.close()

            evt = models.Event.from_elm(elm)
        except (models.UnmarshalError, etree.XMLSyntaxError) as exc:
            self.lgr.warning("Unable to parse Event from persistence store: %s", exc)
            purge = True
        except redis.ResponseError as exc:
            self.lgr.warning("Unable to get Event from persistence store: %s", exc)
            purge = True
        except redis.ConnectionError as exc:
            self._redis_result(False)
            return None
        except Exception as exc:  # pylint: disable=broad-except
            self.lgr.error(
                "Uhandled exception parsing Event from persistence store: %s", exc
            )
            purge = True

        if purge:
            self.lgr.warning("Purging key %s", key)
            try:
                self.rds.delete(key)
            except:  # pylint: disable=bare-except
                pass
            return None

        return evt

    def get_all(self):
        try:
            for key in self.rds.keys(f"{self.rds_ks}:*"):
                evt = self._get_event(key, True)
                if evt:
                    yield evt
        except redis.ConnectionError:
            self._redis_result(False)
            return


class OraclePersistence(BasePersistence):
    """
    Persistence backend for storing CoT events in Oracle Cloud Object Storage.
    Each event is stored as an XML object in a specified bucket.
    Arguments:
      taky_config: taky.config.app_config object with required [oracle] section
    """
    def __init__(self, taky_config):
        super().__init__()

        # Require OCI module
        if oci is None:
            raise ImportError("oci SDK is not installed. Please install 'oci' package.")

        # Required connection parameters for Oracle OCI Object Storage:
        # https://docs.oracle.com/en-us/iaas/tools/python/2.154.0/configuration.html
        self.oci_config = {
            "user": taky_config.get("oracle", "user"),
            "key_file": taky_config.get("oracle", "keyfile_path"),
            "fingerprint": taky_config.get("oracle", "fingerprint"),
            "tenancy": taky_config.get("oracle", "tenancy"),
            "region": taky_config.get("oracle", "region")}
        
        self.compartment_id = taky_config.get("oracle", "compartment")
        self.bucket_name = taky_config.get("oracle", "bucket_name")
        self.prefix = taky_config.get("oracle", "prefix")
        self.is_config_valid = False
        self.namespace = ''

        self.client = self.create_client()
    
    def create_client(self) -> oci.object_storage.ObjectStorageClient:
        """ Create an OCI client object that can be used to perform operations """
        if not self.is_config_valid:
            self.validate_config()
        self.client = oci.object_storage.ObjectStorageClient(self.oci_config)
        return self.client
    
    def validate_config(self) -> None:
        """ Uses OCI API to validate Object Storage configuration """
        oci.config.validate_config(self.oci_config)  # https://docs.oracle.com/en-us/iaas/tools/python/2.154.0/api/config.html#oci.config.validate_config
        self.is_config_valid = True
        return
    
    def get_namespace(self) -> str:
        """ Retrieves the immutable namespace for the Oracle Cloud Storage instance """
        response = self.client.get_namespace()
        if response:  # keep type checker from choking
            self.namespace = response.data
        return self.namespace
    
    def check_bucket_exists(self) -> bool:
        """
        Checks to see if a bucket exists by looking for it in list of all buckets.
        Could potentially be slow on a large Object Storage instance.
        """
        if not self.client:
            self.create_client()
        if not self.namespace:
            self.get_namespace()
        
        response = self.client.list_buckets(self.namespace, self.compartment_id)
        
        if response:
            allbuckets = response.data
        else:
            raise ValueError(f"Could not retrieve list of OCI buckets for compartment {self.compartment_id} in namespace {self.namespace}")

        for b in allbuckets:
            if b.name == self.bucket_name:
                self.bucket_exists = True
        return self.bucket_exists
    
    def create_bucket(self) -> str:
        """
        Creates the bucket specified in the configuration.
        Should be called only if the bucket doesn't already exist.
        """
        if not self.client:
            self.create_client()
        if not self.namespace:
            self.get_namespace()
        
        response = self.client.create_bucket(
            namespace_name=self.namespace,
            create_bucket_details=oci.object_storage.models.CreateBucketDetails(
                name=self.bucket_name,
                compartment_id=self.compartment_id
            )
        )

        self.bucket_exists = True
        return response.data.name
    
    def _get_key(self, uid):
        """ Generate the object key by adding prefix for a given event UID. """
        if self.prefix:
            return f"{self.prefix}/{uid}"
        else:
            return uid
    
    def get_event(self, uid, uid_is_key=False):
        """
        Retrieve an event by UID from Oracle Object Storage.
            uid (str): Event UID.
            uid_is_key (bool): Use provided UID as Object Storage key (don't add prefix)
        Returns parsed event XML, or None if not found.
        """
        if uid_is_key:
            key = uid
        else:
            key = self._get_key(uid)  # adds prefix (if prefix non-null)
        
        evt = None

        try:
            response = self.client.get_object(self.namespace, self.bucket_name, key)
            if not response or not hasattr(response, "data") or response.data is None:
                return None
            xml_data = response.data.read()
            if xml_data is None:
                return None
        except oci.exceptions.ServiceError as e:
            if e.status == 404:
                return None  # Object not found
            raise  # TODO: Log or handle other errors...

        try:
            parser = etree.XMLParser(resolve_entities=False)
            parser.feed(xml_data)
            elm = parser.close()
            evt = models.Event.from_elm(elm)
        except (models.UnmarshalError, etree.XMLSyntaxError) as exc:
            self.lgr.warning("Error while parsing Event: %s", exc)
        
        return evt
    
    def track_event(self, event, ttl=None):
        """ 
        Currently just a compatibility alias to _set_event() 
          ttl argument is unused
        """
        try:
            return self._set_event(event.uid, event)
        except AttributeError:
            return self._set_event(None, event)

    def _set_event(self, uid, event):
        xml_bytes = etree.tostring(event.as_element).encode("utf-8")
        if uid:
            object_name = self._get_key(uid)
        else:
            uid = hashlib.shake_128(xml_bytes).hexdigest(16)
            object_name = self._get_key(uid)
        
        try:
            self.client.put_object(
                self.namespace,
                self.bucket_name,
                object_name,
                io.BytesIO(xml_bytes)
            )
        except oci.exceptions.ServiceError as e:
            raise  # Log or handle upload errors

        return True
    
    def del_event(self, event):
        return self._del_event(event.uid)

    def _del_event(self, uid):
        object_name = self._get_key(uid)
        try:
            self.client.delete_object(self.namespace, self.bucket_name, object_name)
        except oci.exceptions.ServiceError as e:
            if e.status != 404:
                raise  # Ignore not found, raise other errors
        return True

    def _get_objects(self, search_prefix):
        try:
            # TODO: Responses may be paginated and need to be walked through, & could be very large
            objects = self.client.list_objects(self.namespace, self.bucket_name, prefix=search_prefix).data.objects
        except oci.exceptions.ServiceError as e:
            raise
        return objects

    def get_all(self):
        """
        List all event UIDs stored in Oracle Object Storage bucket (with prefix, if enabled).
        """
        objects = self._get_objects(self.prefix)
            
        if self.prefix:
            uids = [obj.name[len(self.prefix)+1:] for obj in objects]
        else: 
            uids = [obj.name for obj in objects]
        
        return uids
    
    def event_exists(self, uid):
        # TODO: Placeholder for testing
        raise NotImplementedError

    def prune(self):
        # TODO: Placeholder for testing; should prune object store of stale/expired items
        raise NotImplementedError
