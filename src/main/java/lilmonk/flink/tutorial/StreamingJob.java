/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lilmonk.flink.tutorial;

import lilmonk.flink.tutorial.datagen.TemperatureSensor;
import lilmonk.flink.tutorial.model.Sensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // This is used by default.

        // assign fictitious stream of data as our data source and extract timestamp field
        /*DataStream<Sensor> inputStream = env.addSource(new TemperatureSensor())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Sensor>() {
                    @Override
                    public long extractAscendingTimestamp(Sensor sensor) {
                        return sensor.getTimestamp();
                    }
                });*/

        DataStream<Sensor> inputStream = env.addSource(new TemperatureSensor())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Sensor>forMonotonousTimestamps()
                        .withTimestampAssigner((sensor, ts) -> sensor.getTimestamp()));

        DataStream<Double> temps = inputStream.map(new MapFunction<Sensor, Double>() {
            @Override
            public Double map(Sensor sensor) throws Exception {
                return sensor.getTemperature();
            }
        });

        /*// define a simple pattern and condition to detect from data stream
        Pattern<Sensor, ?> highTempPattern = Pattern.<Sensor>begin("first")
                .where(new SimpleCondition<Sensor>() {
                    @Override
                    public boolean filter(Sensor sensor) throws Exception {
                        return sensor.getTemperature() > 60;
                    }
                });

        // get resulted data stream from input data stream based on the defined CEP pattern
        DataStream<Sensor> result = CEP.pattern(inputStream.keyBy(new KeySelector<Sensor, Integer>() {
                    @Override
                    public Integer getKey(Sensor sensor) throws Exception {
                        return sensor.getDeviceId();
                    }
                }), highTempPattern)
                .process(new PatternProcessFunction<Sensor, Sensor>() {
                    @Override
                    public void processMatch(Map<String, List<Sensor>> match, Context ctx, Collector<Sensor> collector) throws Exception {
                        collector.collect(match.get("first").get(0));
                    }
                });*/

        temps.print("result");


        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
