# IOT Multiple Sinks
 Spark Streaming IOT Multiple Sinks

Dataset Story:

The data was generated from a series of three identical, custom-built, breadboard-based sensor arrays. Each array was connected to a Raspberry Pi devices. Each of the three IoT devices was placed in a physical location with varied environmental conditions.

Goal:

Using SparkStructuredStreaming, separate the signals from three different sensors and print them to three different sinks.(hive, postgresql and delta)

Data Fields:

ts: timestamp of reading.

device: unique device name.

co:carbon monoxide.

humidity: humidity (%)

light: light detected?

lpg: liquefied petroleum gas

motion: motion detected?

smoke: smoke

temp:temperature(F)
