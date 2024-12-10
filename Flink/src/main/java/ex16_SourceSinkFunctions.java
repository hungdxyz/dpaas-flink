
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class ex16_SourceSinkFunctions {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Tuple3<String,Double,Double>> streamSource = env.addSource(new StockSource("MSFT"));

        streamSource.addSink(new ExcelSink("/Users/swethakolalapudi/flinkJar/testExcel.xlsx"));


        env.execute("Custom Source and Sink");

    }

    public final static class StockSource implements SourceFunction<Tuple3<String,Double,Double>> {

        private String symbol;
        private int count;


        public StockSource(String symbol) {
            this.symbol = symbol;
            this.count=0;
        }

        public void run(SourceContext<Tuple3<String,Double,Double>> sourceContext) throws Exception {

            while(count<10){

                try
                {

                    StockQuote quote = YahooFinance.get(symbol).getQuote();
                    BigDecimal price = quote.getPrice();
                    BigDecimal prevClose = quote.getPreviousClose();
                    sourceContext.collect(Tuple3.of(symbol, price.doubleValue(), prevClose.doubleValue()));
                    count+=1;
//                    sourceContext.collectWithTimestamp(
//                            Tuple3.of(symbol, price.doubleValue(), prevClose.doubleValue()),
//                            System.currentTimeMillis());

                }
                catch(Exception e) {}

            }

        }

        public void cancel() {

        }



    }

    public final static class ExcelSink extends RichSinkFunction<Tuple3<String,Double,Double>> {


        private final String filePath;
        private Map<Integer, Object[]> dataInRows;
        private Integer lastRowNum;

        public ExcelSink(String filePath) {
            this.filePath = filePath;
        }

        public void open(Configuration parameters) throws Exception {

            dataInRows = new HashMap<Integer, Object[]>();

            dataInRows.put(1, new Object[]{"Ticker", "Close", "Prev Close"});
            lastRowNum = 2;

        }

        public void invoke(Tuple3<String, Double, Double> stockPrice) throws Exception {

            dataInRows.put(lastRowNum, new Object[]{stockPrice.f0, stockPrice.f1, stockPrice.f2});
            lastRowNum += 1;
        }

        public void close() throws Exception {

            writeToExcel(lastRowNum, dataInRows, filePath);
        }

        private static void writeToExcel(Integer lastRowNum, Map<Integer, Object[]> dataInRows, String filePath) throws IOException {
            HSSFWorkbook workbook = new HSSFWorkbook();

            HSSFSheet worksheet = workbook.createSheet("Data");

            for (int currrentRow = 0; currrentRow < lastRowNum; currrentRow++) {
               try{
                Integer key = (currrentRow + 1);
                Row row = worksheet.createRow(currrentRow + 1);
                Object[] values = dataInRows.get(key);
                int cellNum = 0;
                for (Object oneObject : values) {
                    Cell cell = row.createCell(cellNum++);
                    if (oneObject instanceof String) {
                        cell.setCellValue((String) oneObject);
                    } else if (oneObject instanceof Double) {
                        cell.setCellValue((Double) oneObject);
                    } else if (oneObject instanceof Date) {
                        cell.setCellValue((Date) oneObject);
                    } else if (oneObject instanceof Boolean) {
                        cell.setCellValue((Boolean) oneObject);
                    }
                }}
               catch (Exception e){

               }
            }

            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(new File(filePath));
                workbook.write(fos);

            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                fos.close();
            }
        }

    }

}
