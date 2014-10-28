# SimpleArchive

SimpleArchive is a very simple document store which also tracks metadata.
It is intended for use in parallel tests and therefore performs locking on the
metadata file.

This is NOT intended for production but really only for smallish apps and unit test!
If you need a faster and much more reliable document archive then consider
using Apache JackRabbit. 
