import os
import logging
import configparser

app_config = configparser.ConfigParser(allow_no_value=True)

DEFAULT_CFG = {
    "taky": {
        "hostname": "taky.local",  # Server's FQDN
        "node_id": "TAKY",  # This server's TAK nodeId
        "bind_ip": None,  # Bind address; if None, defaults to all (0.0.0.0) interfaces
        "public_ip": None,  # Server's public IP address
        "persistence": "memory",  # Persistance backend to use
        "root_dir": "/var/taky",  # Where the root taky directory lies
    },
    "redis": {
        "server": "localhost"  # Default Redis server to use if persistence=redis
    },
    "oracle": {
        # TODO: Oracle Object Storage backend default config params
    },
    "cot_server": {
        "port": None,  # Defaults to 8087 (or 8089 if SSL)
        "mon_ip": None,
        "mon_port": None,
        "log_cot": None,  # Path to log COT files to
        "max_persist_ttl": -1,  # Enforce a maximum persistence TTL
    },
    "dp_server": {
        "upload_path": "/var/taky/dp-user",
    },
    "ssl": {
        "enabled": False,
        "client_cert_required": True,
        "ca": "/etc/taky/ssl/ca.crt",
        "ca_key": "/etc/taky/ssl/ca.key",
        "server_p12": "/etc/taky/ssl/server.p12",
        "server_p12_pw": "atakatak",
        "cert": "/etc/taky/ssl/server.crt",
        "key": "/etc/taky/ssl/server.key",
        "key_pw": None,
        "cert_db": "/etc/taky/ssl/cert-db.txt",
    },
}


def load_config(path=None, explicit=False):
    """
    Loads a config file from the specified path into the global config. If no
    path is specified, the file is inferred by checking local and global paths.

    @param path     The path of the configuration file to load
    @param explicit Don't return a default
    """
    lgr = logging.getLogger("load_config")

    ret_config = configparser.ConfigParser(allow_no_value=True)
    ret_config.read_dict(DEFAULT_CFG)  # load default values into ret_config

    if path is None:
        if explicit:
            raise FileNotFoundError("Config file explicitly required, but not present")
        elif os.path.exists("taky.conf"):
            path = os.path.abspath("taky.conf")
            lgr.info(f"Config file found at {path}")
        elif os.path.exists("/etc/taky/taky.conf"):
            path = "/etc/taky/taky.conf"
            lgr.info(f"Config file found at {path}")
        else:
            raise FileNotFoundError("No config file found in default locations")

    if path and os.path.exists(path):
        lgr.info("Loading config file from %s", path)
        cfg_dir = os.path.realpath(os.path.dirname(path))
        with open(path, "r", encoding="utf8") as cfg_fp:
            ret_config.read_file(cfg_fp, source=path)  # read config file into ret_config
    elif explicit:
        raise FileNotFoundError("Config file explicitly required, but not present")
    else:
        lgr.info("Using default config")
        cfg_dir = os.getcwd()
        ret_config.set("taky", "root_dir", ".")
        ret_config.set("dp_server", "upload_path", "./dp-user")

    # Make dir paths absolute
    for (sect, opt) in [
        ("taky", "root_dir"),
        ("dp_server", "upload_path"),
        ("cot_server", "log_cot"),
    ]:
        path = ret_config.get(sect, opt)
        if path and not os.path.isabs(path):
            path = os.path.realpath(os.path.join(cfg_dir, path))
            ret_config.set(sect, opt, path)

    # Validate port selection
    port = ret_config.get("cot_server", "port")
    if port in [None, ""]:
        if ret_config.getboolean("ssl", "enabled"):
            port = 8089
        else:
            port = 8087
    else:
        try:
            port = int(port)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid port: {port}") from exc

        if port <= 0 or port >= 65535:
            raise ValueError(f"Invalid port: {port}")
    ret_config.set("cot_server", "port", str(port))

    # Validate max TTL
    max_ttl = ret_config.get("cot_server", "max_persist_ttl")
    if max_ttl in [None, ""]:
        max_ttl = -1
    else:
        try:
            max_ttl = int(max_ttl)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid max_persist_ttl: {max_ttl}") from exc
    ret_config.set("cot_server", "max_persist_ttl", str(max_ttl))

    # Validate SSL-related settings
    if not ret_config.getboolean("ssl", "enabled"):
        ret_config.set("cot_server", "mon_ip", None)  # No monitor IP if SSL is disabled
        ret_config.set("cot_server", "mon_port", None)
    else:
        port = ret_config.get("cot_server", "mon_port")
        if port in [None, ""]:
            port = 8087
        else:
            try:
                port = int(port)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid port: {port}") from exc
            if port <= 0 or port >= 65535:
                raise ValueError(f"Invalid port: {port}")
        ret_config.set("cot_server", "mon_port", str(port))

        for f_name in ["ca", "ca_key", "server_p12", "cert", "key", "cert_db"]:
            f_path = ret_config.get("ssl", f_name)
            if f_path and not os.path.isabs(f_path):
                f_path = os.path.realpath(os.path.join(cfg_dir, f_path))
                ret_config.set("ssl", f_name, f_path)

    if explicit:
        ret_config.set("taky", "cfg_path", path)

    app_config.clear()
    app_config.update(ret_config)
