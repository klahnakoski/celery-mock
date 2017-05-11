# celery-mock
A Celery mock for Redash

## Overview

Reduce setup overhead for your development machine by using this Celery mock instead. Works on Windows too!!

### Celery does not work on Windows

Celery has complexity regarding the spwning of child processes: It is assumed that the parent state is copied to the children
(a Linux assumption), this is a bad assumption because it does not hold true for Windows, and special care must be taken to ensure
locks are freed after the spwn.  This also introducea

### Celery requires yet-another-service



Redash requires Celery, which in turn requires a message passing service. Celery (and it's billiard library) have complexity regarding 
