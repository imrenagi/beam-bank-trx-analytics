Beam Bank Transaction Analytics
===

This repo contains small example for doing streaming analytics. The use case picked up is use case of money transfer event 
from an accountID to another accountID with a particular amount of money and an event timestamp.

---
There is two types of analytics windowing used:

#### Globally Aggregated Transaction
Analytics is done in global window. Here, global means unbounded window. So, all aggregated
data created through this mode is considered as the sum of data
from the beginning to endless time window.
 
#### Periodically Aggregated Transaction
This type of windowing can be configured into a certain time window. Which means, now we can 
calculate the aggregated value for a certain period of time, for instance per minute, per hour, per week,
and so on. 

> Both mode mentioned above uses speculative aggregate trigger. This means
in a designated period of time within a window, the application 
will trigger one or more speculative aggregates from the data which have arrived on
to the system.

## Getting Started

* Install maven (the newer version, the better)
* Run `mvn test` to execute the test.
* To run the application, chose either `StreamingPipeline.java` or `BatchPipeline.java`.

## References
1. Apache Beam Game [Example](https://github.com/apache/beam/tree/master/examples)
2. Apache Beam [Documentation](https://beam.apache.org/documentation/)  