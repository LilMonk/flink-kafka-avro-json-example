package lilmonk.flink.tutorial.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class PageViewJson {
    private long viewtime;
    private String userid;
    private String pageid;
}
