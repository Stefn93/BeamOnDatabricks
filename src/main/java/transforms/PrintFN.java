package transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintFN {

    private final static Logger LOG = LoggerFactory.getLogger(PrintFN.class);

    public static DoFn<String, String> printString = new DoFn<String, String>() {
        @ProcessElement
        public void processElement(@Element String wordcount, OutputReceiver<String> out) {
            LOG.info(wordcount);
            out.output(wordcount);
        }
    };

}
