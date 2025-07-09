# Jachimo's fork of *taky*

taky - A simple COT server for ATAK

## Current Changes From Upstream

This is a highly experimental, development-grade version of Taky, with a handful of
changes that may or may not be of interest:

 * Oracle Cloud Infrastructure (OCI) Object Storage backend added
   * Uses the official Oracle Cloud SDK to persist CoT messages
   * Oracle SDK not required, if you aren't using it (Redis made optional, too)
 * Import statements updated for PEP 420 compliance
   * Builds (with many complaints) using recent Setuptools
   * First step towards PEP 518 builds
 * Tested (lightly) against Python 3.10

Unless these changes are of significant utility to you, and you want to test them
and provide feedback, you are probably better off using the upstream version:
[tkuester/taky](https://github.com/tkuester/taky).
