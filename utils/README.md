# Utils

## nats-wiretap [Ref: EIP - Hohpe & Woolfe, 547]

``` bash
python nats-wiretap.py [queue-spec]
```
Example of non-intrusive wiretap to specified queue / queue-spec wrapped as a separate CLI.
Each message received on the wiretap is printed to stdio.  This would facilitate command 
chaining using standard unix pipes / filters etc if needed. 

### Usage Examples:

1. python nats-wiretap.py p1.s1 - Listen to Pipeline P1, Stage s1.
2. python nats-wiretap.py p1.*  - Listen to Pipleine P1 across all stages.

## nats-aggregator [Ref: EIP - Hohpe & Woolfe, 268]

``` bash
python nats-aggregator.py [queue-spec]
```
Example of stateful aggregator.  State is held in process as a dict, 
checked on receipt of each message with an aggregated summary published to a new queue 
when each of the constituent messages have been received.