package lilmonk.flink.tutorial.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Sensor {
    private int deviceId;
    private double temperature;
    private long timestamp;
}