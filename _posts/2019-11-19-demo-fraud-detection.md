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

These patterns expand the possibilities of what is possible with statically defined data flows and therefore provide important building blocks to fulfil versatile business requirements.

**Dynamic updates of application logic** allow Flink jobs to change at runtime, without downtime from stopping and resubmitting the code.  
<br>
**Dynamic data partitioning** enables the ability to change how events are being distributed and grouped by the `keyBy()` operator in Flink at runtime. Such functionality often becomes a natural requirement when building jobs with dynamically reconfigurable application logic.  
<br>
**Custom window management** demonstrates how you can utilize the low level [ProcessFunction API](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/process_function.html), when [Window API](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html) is not exactly matching your requirements. Specifically, you will learn how to implement low latency alerting on windows and how to limit state growth with timers.    

The patterns are built up on top of core Flink functionality, however they might not be immediately obvious from the framework's documentation as it is not trivial to explain and give motivation behind them without a concrete example use case. For this reason we will look at the details of the patterns on an example of building an application which represents a common use case for Apache Flink - a _Fraud Detection_ engine.
We hope this series will put these powerful approaches into your tool belt and enable you to tackle new interesting use cases with Apache Flink.

In the first blog post of the series we will look at the high level architecture of the demo application, describe its components and their interactions. We will proceed with a deep dive into the implementation details of the first pattern in the series - **dynamic data partitioning**.

You will be able to run the full Fraud Detection Demo application locally and look into the details of the implementation by using the accompanying GitHub repository.

### Fraud Detection Demo

If you are keen on seeing the demo application in action, feel free to checkout the following repository and follow the steps in the README to run it locally:

[https://github.com/afedulov/fraud-detection-demo](https://github.com/afedulov/fraud-detection-demo)

Our Fraud Detection Demo comes in a form of a self-contained application - it only requires `docker` and `docker-compose` to be built from sources and includes the following components:

 - Apache Kafka (message broker) with ZooKeeper
 - Apache Flink ([application cluster](https://ci.apache.org/projects/flink/flink-docs-stable/concepts/glossary.html#flink-application-cluster))
 - Fraud Detection Web App  

The high level goal of the Fraud Detection engine is to consume a stream of financial transactions and evaluate a set of rules against it. Such rules are typically subject to frequent changes and tweaks. In a real production system it is therefore very important to be able to add and removed them at runtime, without incurring an expensive penalty of stopping and restarting the job.

When you navigate to the demo URL in your browser, you will be presented with the following UI:

 <center>
 <img src="{{ site.baseurl }}/img/blog/2019-11-19-demo-fraud-detection/ui.png" width="800px" alt="Figure 1: Demo UI"/>
 <br/>
 <i><small>Figure 1: Fraud Detection Demo UI</small></i>
 </center>
 <br/>

On the left side you can see a visual representation of financial transactions flowing through the system after you click the "Start" button. The slider at the top allows you to control the number of transactions per second generated for the demo purposes. The middle section is devoted to managing the rules evaluated by Flink. In this section you can create new rules as well as issue certain control commands, such as clearing Flink's state.

The demo out already of the box comes with a set of predefined sample rules. You can simply click the _Start_ button and after some time observe alerts displayed in the right section of the UI. These alerts are the result of Flink evaluating the generated transactions stream against the predefined rules.

 Our sample fraud detection application consists of three main components:

  1. Frontend (React)  
  1. Backend (SpringBoot)  
  1. Fraud Detection application (Apache Flink)  

Interactions between the main elements is depicted on _Figure 2_.

 <center>
 <img src="{{ site.baseurl }}/img/blog/2019-11-19-demo-fraud-detection/architecture.png" width="800px" alt="Figure 2: Demo Components"/>
 <br/>
 <i><small>Figure 2: Fraud Detection Demo Components</small></i>
 </center>
 <br/>

 Backend exposes a REST API to the Frontend for creating and deleting rules as well as issuing control commands for managing the demo execution. It then relays those Frontend actions to Flink by sending them via a "Control" Kafka topic. Backend also includes a Transactions Generator component, which sends emulated flow of funds to Flink via a separate "Transactions" topic. Alerts generated by Flink are consumed by the Backend from "Alerts" topic and relayed to the UI via WebSockets.

Now that you are familiar with the overall layout and the goal of our Fraud Detection engine, let's go into the details of what is required to implement such system.

### Dynamic Data Partitioning

The first pattern we will look into is Dynamic Data Partitioning.

Flink's DataStream API includes an important operator that determines the distribution of incoming data (shuffle) among the parallel computational nodes of the cluster - the `keyBy()` operator. It partitions the stream in a way that all records with the same key are assigned to the same partition and are hence sent to the same instance of the subsequent "keyed" operator. In a typical statically-defined streaming application the choice of the key is fixed and it is usually immediately derived from one of the fields of the incoming records. For instance, in case of a simple window-based computation over a stream of Tuples, we could use the first field (denoted by 0) as the key:

```java
DataStream<Tuple2<String, Integer>> input = // [...]
DataStream<...> windowed = input
  .keyBy(0)
  .window(/*window specification*/);
```

This is the basic building block for achieving horizontal scalability which covers a wide range of use cases. However, in a case of an application that strives to provide flexibility in business logic at runtime, this is not enough.
To understand why this is the case, let us start with articulating a realistic sample rule definition for our fraud detection system in a form of a functional requirement:  

"Whenever the **sum** of accumulated **payment amount** from the same **beneficiary** to the same **payee** within the **duration of a week** is **greater** than **1 000 000 $** - fire an alert."

This rule shows a number of parameters that we would like to be flexible and expose for any (new) rule that is being sent to our fraud detection application during its runtime:

From this formulation we can extract the following parameters that we would like to be able to specify in a system which allows flexibility in rules definition:  

1. Aggregation field (payment amount)  
1. Grouping fields (beneficiary + payee)  
1. Aggregation function (sum)  
1. Window duration (1 week)  
1. Limit (1 000 000)  
1. Limit operator (greater)  

Accordingly, we will use the following simple JSON format to define the aforementioned parameters.

Examples JSON:

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

At this point it is important to understand that **`groupingKeyNames`** determine the actual physical grouping of events - all Transactions with the same values of specified parameters (e.g. _beneficiary #25 -> payee #12_ have to be aggregated in the same parallel instance of the evaluating operator. Naturally, the process of distributing data in such manner in Flink's API is realised by a `keyBy()` function.

Although most examples in the [documentation](https://ci.apache.org/projects/flink/flink-docs-stable/dev/api_concepts.html#define-keys-using-field-expressions) use specific fixed events' fields, nothing prevents us from extracting them in a more dynamic fashion, based on the specifications of the rules. For this we will need one additional preparation step before invoking the `keyBy()` function.

Let's look at a high level how our main processing pipeline might look like:

```java
DataStream<Alert> alerts =
    transactions
        .process(new DynamicKeyFunction())
        .keyBy(/* some key selector */);
        .process(/* actual calculations and alerting */)
```

Given a set of predefined rules in the first step of the processing pipeline, we would like to iterate over them and prepare every event to be dispatched to a respective aggregating instance. This is what is done in `DynamicKeyFunction`:

```java
public class DynamicKeyFunction
    extends ProcessFunction<Transaction, Keyed<Transaction, String, Integer>> {
   ...
  /* Simplified */
  List<Rule> rules = /* Rules that are initialized somehow.
                        Details will be discussed in a separate section. */;

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
 `KeysExtractor.getKey()` uses reflection to extract required values of `groupingKeyNames` fields from events and combines them as a single concatenated String key, e.g `{beneficiaryId=25;payeeId=12}`.

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

Where `wrapped` is the original Transaction event data, `key` is the result of using `KeysExtractor` and `id` is the ID of the Rule which caused the dispatch of the event according to this Rule's grouping logic.

Events of this type will be the input to the `keyBy()` function of the main processing pipeline. This allows us to use a simple lambda-expression in place of a [`KeySelector`](https://ci.apache.org/projects/flink/flink-docs-stable/dev/api_concepts.html#define-keys-using-key-selector-functions) as the final step of implementing dynamic data shuffle.

```java
DataStream<Alert> alerts =
    transactions
        .process(new DynamicKeyFunction())
        .keyBy((keyed) -> keyed.getKey());
        .process(/* actual calculations and alerting */)
```

Effectively we are forking events for parallel evaluation in the Flink cluster. There is a certain amount of data duplication which is required for making use of Flink's parallel model of execution. In a real-life scenario, an additional layer for filtering and propagating only those Transaction fields which are actually needed for evaluation of specific rules could be applied to decrease this impact.

<center>
<img src="{{ site.baseurl }}/img/blog/2019-11-19-demo-fraud-detection/shuffle.png" width="800px" alt="Figure 3: Events Shuffle"/>
<br/>
<i><small>Figure 3: Events Shuffle</small></i>
</center>
<br/>


Building a fully fledged DSL and a rules engine is not the focus of this post, hence this part is be kept to a minimum of what is required to show case intended functionality. You can, however, think about implementing more sophisticated rules, including filtering of certain events, rules chaining and other more advanced scenarios.

In the next part of this series of blog posts, we will look into how these rule sets make it into Flink and how they can be added and removed at runtime (the Dynamic Application Updates pattern).
