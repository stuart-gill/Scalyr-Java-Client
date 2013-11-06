Scalyr Java Client Library
---

This is the source code for the Java client to the Scalyr Logs and Scalyr Knobs services.
See [scalyr.com/logapijava](https://www.scalyr.com/logapijava) for an introduction to the
API.

We do not actively solicit outside contributions to the client library, but if you'd
like to submit a patch, feel free to get in touch (contact@scalyr.com). And of course,
feedback, requests, and suggestions are always welcome!



json-simple
---

The com.scalyr.api.json package contains a bastardized copy of the json-simple library
(http://code.google.com/p/json-simple/). We have removed code not needed for our
purposes, and renamed the package to avoid conflicts. Thanks to Yidong Fang and Chris
Nokleberg, the authors of json-simple, for this very handy library.
