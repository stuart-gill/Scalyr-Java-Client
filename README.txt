Here is the source code to the Scalyr client library. We provide this code for
several reasons:

1. So that engineers who really like to understand how things work, can see
the inner workings.

2. As a debugging aid.

3. As sample code for using Scalyr services from a language for which we have
not yet developed an official client.

We do not actively solicit outside contributions to the client library, but if you'd
like to submit a patch, feel free to get in touch (contact@scalyr.com). And of course,
feedback, requests, and suggestions are always welcome!



json-simple

The com.scalyr.api.json package contains a bastardized copy of the json-simple library
(http://code.google.com/p/json-simple/). We have removed code not needed for our
purposes, and renamed the package to avoid conflicts. Thanks to Yidong Fang and Chris
Nokleberg, the authors of json-simple, for this very handy library.
