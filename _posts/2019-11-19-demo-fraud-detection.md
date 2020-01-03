---
layout: post
title: "Building a Fraud Detection System with Runtime Rules Updates on Apache Flink"
date: 2019-11-19T12:00:00.000Z
authors:
- alex:
  name: "Alexander Fedulov"
  twitter: "afedulov"
excerpt:
categories: news
---

In this series of blog posts you will learn about three powerful Flink patterns for building streaming applications:

 - Dynamic updates of application logic
 - Dynamic data partitioning (shuffle), controlled at runtime
 - Low latency alerting based on custom window logic (not using the Window API)

These patterns expand the possibilities of what is achievable with statically defined data flows and thus provide important building blocks to fulfil versatile business requirements.

**Dynamic updates of application logic** allow Flink jobs to change at runtime, without downtime from stopping and resubmitting the code.  
<br>
**Dynamic data partitioning** enables the ability to change how events are being distributed and grouped by the `keyBy()` operator in Flink at runtime. Such functionality often becomes a natural requirement when building jobs with dynamically reconfigurable application logic.  
<br>
**Custom window management** demonstrates how you can utilize the low level [ProcessFunction API](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/process_function.html), when [Window API](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html) is not exactly matching your requirements. Specifically, you will learn how to implement low latency alerting on windows and how to limit state growth with timers.    

The patterns are built up on top of core Flink functionality, however they might not be immediately obvious from the framework's documentation as it is not trivial to explain and give motivation behind them without a concrete example use case. For this reason we will look at the details of the patterns on an example of building an application which represents a common usage scenario for Apache Flink - a _Fraud Detection_ engine.
We hope this series will put these powerful approaches into your tool belt and enable you to tackle new tasks with Apache Flink.

In the first blog post of the series we will look at the high level architecture of the demo application, describe its components and their interactions. We will proceed with a deep dive into the implementation details of the first pattern in the series - **dynamic data partitioning**.

You will be able to run the full Fraud Detection Demo application locally and look into the details of the implementation by using the accompanying GitHub repository.

### Fraud Detection Demo

If you are keen on seeing the demo application in action, feel free to checkout the following repository and follow the steps in the README to run it locally:

[https://github.com/afedulov/fraud-detection-demo](https://github.com/afedulov/fraud-detection-demo)

Our Fraud Detection Demo comes in a form of a self-contained application - it only requires `docker` and `docker-compose` to be built from sources and includes the following components:

 - Apache Kafka (message broker) with ZooKeeper
 - Apache Flink ([application cluster](https://ci.apache.org/projects/flink/flink-docs-stable/concepts/glossary.html#flink-application-cluster))
 - Fraud Detection Web App  

The high level goal of the Fraud Detection engine is to consume a stream of financial transactions and evaluate a set of rules against it. Such rules are typically subject to frequent changes and tweaks. In a real production system it is therefore important to be able to add and removed them at runtime, without incurring an expensive penalty of stopping and restarting the job.

When you navigate to the demo URL in your browser, you will be presented with the following UI:

 <center>
 <img src="{{ site.baseurl }}/img/blog/2019-11-19-demo-fraud-detection/ui.png" width="800px" alt="Figure 1: Demo UI"/>
 <br/>
 <i><small>Figure 1: Fraud Detection Demo UI</small></i>
 </center>
 <br/>

On the left side you can see a visual representation of financial transactions flowing through the system after you click the "Start" button. The slider at the top allows you to control the number of transactions per second generated for the demo purposes. The middle section is devoted to managing the rules evaluated by Flink. In this section you can create new rules as well as issue certain control commands, such as clearing Flink's state.

The demo out of the box comes with a set of predefined sample rules. You can
click the _Start_ button and after some time will observe alerts displayed in the right section of the UI. These alerts are the result of Flink evaluating the generated transactions stream against the predefined rules.

 Our sample fraud detection system consists of three main components:

  1. Frontend (React)  
  1. Backend (SpringBoot)  
  1. Fraud Detection application (Apache Flink)  

Interactions between the main elements are depicted in _Figure 2_.

 <center>
 <img src="{{ site.baseurl }}/img/blog/2019-11-19-demo-fraud-detection/architecture.png" width="800px" alt="Figure 2: Demo Components"/>
 <br/>
 <i><small>Figure 2: Fraud Detection Demo Components</small></i>
 </center>
 <br/>

 Backend exposes a REST API to the Frontend for creating and deleting rules as well as issuing control commands for managing the demo execution. It then relays those Frontend actions to Flink by sending them via a "Control" Kafka topic. Backend also includes a _Transactions Generator_ component, which sends an emulated stream of money transfer events to Flink via a separate "Transactions" topic. Alerts generated by Flink are consumed by the Backend from "Alerts" topic and relayed to the UI via WebSockets.

Having the familiarity with the overall layout and the goal of our Fraud Detection engine, let's now go into the details of what is required to implement such system.

### Dynamic Data Partitioning

The first pattern we will look into is Dynamic Data Partitioning.

Flink's DataStream API includes an important operator that determines the distribution of incoming data (shuffle) among the parallel computational nodes of the cluster - the **keyBy()** operator. It partitions the stream in such a way that all records with the same key are assigned to the same partition and are hence sent to the same physical instance of the subsequent "keyed" operator.

In a typical statically-defined streaming application the choice of the key is fixed and it is usually immediately derived from one of the fields of the incoming records. For instance, in case of a simple window-based computation over a stream of Tuples, we could use the first field of type String (denoted by 0) as the key:

```java
DataStream<Tuple2<String, Integer>> input = // [...]
DataStream<...> windowed = input
  .keyBy(0)
  .window(/*window specification*/);
```

This approach is the basic building block for achieving horizontal scalability in a wide range of use cases. However, in the case of an application that strives to provide flexibility in business logic at runtime, this is not enough.
To understand why this is the case, let us start with articulating a realistic sample rule definition for our fraud detection system in a form of a functional requirement:  

*"Whenever the **sum** of accumulated **payment amount** from the same **beneficiary** to the same **payee** within the **duration of a week** is **greater** than **1 000 000 $** - fire an alert."*

In this formulation we can spot a number of parameters that we would like to be able to specify in a newly submitted rule and possibly even later modify or tweak at runtime:

1. Aggregation field (payment amount)  
1. Grouping fields (beneficiary + payee)  
1. Aggregation function (sum)  
1. Window duration (1 week)  
1. Limit (1 000 000)  
1. Limit operator (greater)  

Accordingly, we will use the following simple JSON format to define the aforementioned parameters:

```json  
{
  "ruleId": 1,
  "ruleState": "ACTIVE",
  "groupingKeyNames": ["beneficiaryId", "payeeId"],
  "aggregateFieldName": "paymentAmount",
  "aggregatorFunctionType": "SUM",
  "limitOperatorType": "GREATER",
  "limit": 1000000,
  "windowMinutes": 10080
}
```

At this point it is important to understand that **`groupingKeyNames`** determine the actual physical grouping of events - all Transactions with the same values of specified parameters (e.g. _beneficiary #25 -> payee #12_ have to be aggregated in the same physical instance of the evaluating operator. Naturally, the process of distributing data in such a manner in Flink's API is realised by a `keyBy()` function.

Most examples in Flink's `keyBy()`[documentation](https://ci.apache.org/projects/flink/flink-docs-stable/dev/api_concepts.html#define-keys-using-field-expressions) use a hard-coded `KeySelector`, which extracts specific fixed events' fields. However, in order to support desired flexible parameters specification functionality, we have to extract them in a more dynamic fashion based on the specifications of the rules. For this, we will have to use one additional operator that prepares every event for dispatching to a correct aggregating instance.

On a high level, our main processing pipeline looks like this:

```java
DataStream<Alert> alerts =
    transactions
        .process(new DynamicKeyFunction())
        .keyBy(/* some key selector */);
        .process(/* actual calculations and alerting */)
```

We have previously established that each rule defines a **`groupingKeyNames`** parameter. This parameter specifies which combination of fields will be used for the incoming events' grouping. Each rule might use an arbitrary combination of these fields. At the same time, every incoming event potentially needs to be evaluated against multiple rules. This implies that events might simultaneously need to be present at multiple parallel instances of evaluating operators which correspond to different rules and hence will need to be forked. Ensuring such events dispatching is the purpose of `DynamicKeyFunction()`.

<center>
<img src="{{ site.baseurl }}/img/blog/2019-11-19-demo-fraud-detection/shuffle_function_1.png" width="800px" alt="Figure 3: Forking events with Dynamic Key Function"/>
<br/>
<i><small>Figure 3: Forking events with Dynamic Key Function</small></i>
</center>
<br/>

 `DynamicKeyFunction` iterates over a set of defined rules and prepares every event to be processed by a `keyBy()` function by extracting required grouping keys:

```java
public class DynamicKeyFunction
    extends ProcessFunction<Transaction, Keyed<Transaction, String, Integer>> {
   ...
  /* Simplified */
  List<Rule> rules = /* Rules that are initialized somehow.
                        Details will be discussed in a future blog post. */;

  @Override
  public void processElement(
      Transaction event,
      Context ctx,
      Collector<Keyed<Transaction, String, Integer>> out) {

      for (Rule rule :rules) {
       out.collect(
           new Keyed<>(
               event,
               KeysExtractor.getKey(rule.getGroupingKeyNames(), event),
               rule.getRuleId()));
      }
  }
  ...
}
```
 `KeysExtractor.getKey()` uses reflection to extract the required values of `groupingKeyNames` fields from events and combines them as a single concatenated String key, e.g `"{beneficiaryId=25;payeeId=12}"`. Flink will calculate a hash of this key and effectively pin processing of this particular combination to a specific server in the cluster. This will allow tracking all transactions between _beneficiary #25_ and _payee #12_ and evaluating defined rules within a desired time window.

Notice that a wrapper class `Keyed` with the following signature was introduced as the output type of `DynamicKeyFunction`:  

```java   
public class Keyed<IN, KEY, ID> {
  private IN wrapped;
  private KEY key;
  private ID id;

  ...
  public KEY getKey(){
      return key;
  }
}
```

Fields of this POJO carry the following information: `wrapped` is the original transaction's event data, `key` is the result of using `KeysExtractor` and `id` is the ID of the Rule which caused the dispatch of the event according to this Rule's grouping logic.

Events of this type will be the input to the `keyBy()` function in the main processing pipeline. This allows us to use a simple lambda-expression as a [`KeySelector`](https://ci.apache.org/projects/flink/flink-docs-stable/dev/api_concepts.html#define-keys-using-key-selector-functions) for the final step of implementing dynamic data shuffle.

```java
DataStream<Alert> alerts =
    transactions
        .process(new DynamicKeyFunction())
        .keyBy((keyed) -> keyed.getKey());
        .process(new DynamicAlertFunction())

        //.process(/* actual calculations and alerting */)
```

By applying `DynamicKeyFunction` we are effectively copying events for performing parallel per-rule evaluation within a Flink cluster. By doing so, we achieve an important property - horizontal scalability of rules' processing. Our system will be capable of handling more rules by adding more servers to the cluster, i.e. increasing the parallelism. This property is achieved at the cost of data duplication, which might become an issue depending on the specific set of parameters, such as incoming data rate, available network bandwidth, event payload size etc. In a real-life scenario, additional optimizations can be applied, such as combined evaluation of rules which have the same `groupingKeyNames`, or a filtering layer, which would strip events of all the fields that are not required for processing of a particular rule.

### Summary:

In this blog post we have discussed the motivation behind supporting dynamic, runtime changes to a Flink application by looking at a sample use case - a Fraud Detection engine. We have described the overall architecture and interactions between its components as well as provided references for building and running a demo Fraud Detection application in a dockerized setup. We have proceeded with diving into the details of implementing a  **dynamic data partitioning pattern** as the first underlying building block which enables functionality of a flexible fraud detection engine.

For keeping focus on describing the core mechanics of the pattern, we have kept the complexity of the DSL and the underlying rules engine to a minimum. It is, however, easy to think about extensions allowing more sophisticated rules definitions, including filtering of certain events, logical rules chaining and other more advanced functionality.

What we have not yet described is how the rules actually make their way into the running Fraud Detection engine. This will be the topic of the second part of this series of blog posts, where we will look into the implementation details of the main processing function of the pipeline - _DynamicAlertFunction()_.

<center>
<img src="{{ site.baseurl }}/img/blog/2019-11-19-demo-fraud-detection/end-to-end.png" width="800px" alt="Figure 4: End-to-end pipeline"/>
<br/>
<i><small>Figure 4: End-to-end pipeline</small></i>
</center>
<br/>

In the next article we will see how Flink's broadcast streams can be utilized to help steer the processing within the Fraud Detection engine at runtime (Dynamic Application Updates pattern).
