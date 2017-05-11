# celery-mock
A Celery mock for Redash

## Overview

Reduce setup overhead for your development machine by using this Celery mock instead. Works on Windows too!!

### Celery does not work on Windows

Celery has complexity regarding the spwning of child processes in the billiard library: It is assumed that the parent state is copied to the children (a Linux assumption), and this is bad: Special care must be taken to ensure locks are freed after the spawn. This introduces dependency on copied global variables, which does not hold true on Windows. 

### Celery requires yet-another-service

Less services required to get a thing running is better, yes?

## Solution

Mock the Celery API to solve the problems, and make restarts easier.

