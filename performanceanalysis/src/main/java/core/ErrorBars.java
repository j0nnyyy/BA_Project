package core;

import core.containers.DataContainer;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.XChartPanel;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;

import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.util.ArrayList;

public class ErrorBars {

    private static void addLoadSeries(DataContainer container, String series, XYChart chart) {


        int[] workers = {1, 2, 4, 6, 8, 10};

        for(int j = 0; j < workers.length; j++) {
            ArrayList<AppInformation> infoList = container.getWorkerDSSeries(series, workers[j], 16);
            double[] x = new double[infoList.size()];
            double[] y = new double[infoList.size()];
            double[] error = new double[infoList.size()];

            for (int i = 0; i < infoList.size(); i++) {
                x[i] = infoList.get(i).getDataSize();
                y[i] = infoList.get(i).getAvgDuration();
                //error[i] = infoList.get(i).getStdDeviation();
                //System.out.println("X: " + x[i] + ", Y: " + y[i] + ", Error: " + error[i]);
            }

            chart.addSeries(workers[j] + "", x, y);
        }

    }

    private static void addWorkerSeries(String series, XYChart chart, int filecount) {

        String filepath = "C:/Users/Jonas/Desktop/load_count_log.txt";
        DataContainer container = DataContainer.instance();
        container.loadInformation(filepath);
        ArrayList<AppInformation> infoList = container.getDSWorkerSeries(series, filecount, 16);
        double[] x = new double[infoList.size()];
        double[] y = new double[infoList.size()];
        double[] error = new double[infoList.size()];

        for(int i = 0; i < infoList.size(); i++) {
            AppInformation info = infoList.get(i);
            x[i] = info.getWorkers();
            y[i] = info.getAvgDuration();
            error[i] = info.getStdDeviation();
        }

        chart.addSeries(filecount + "", x, y, error);

    }

    private static void compareLoad() {

        String filepath = "C:/Users/Jonas/Desktop/load_log.txt";
        DataContainer container = DataContainer.instance();
        container.loadInformation(filepath);

        XYChart chart = new XYChartBuilder().xAxisTitle("filecount").yAxisTitle("duration(avg)").build();
        //addLoadSeries(container, "load_count_no_schema", chart);
        addLoadSeries(container, "load_count_schema", chart);


        try {
            BitmapEncoder.saveBitmap(chart, "C:/Users/Jonas/Desktop/plots/load_count_schema_comparison", BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void compareSameWorkersAsFiles(String description) {

        String filepath = "C:/Users/Jonas/Desktop/load_log.txt";
        DataContainer container = DataContainer.instance();
        container.loadInformation(filepath);
        ArrayList<AppInformation> infoList = container.getSameWorkersAsFiles(description);

        XYChart chart = new XYChartBuilder().xAxisTitle("workers/files").yAxisTitle("duration(avg)").build();
        double[] x = new double[infoList.size()];
        double[] y = new double[infoList.size()];
        double[] error = new double[infoList.size()];

        for(int i = 0; i < infoList.size(); i++) {
            AppInformation info = infoList.get(i);
            x[i] = info.getWorkers();
            y[i] = info.getAvgDuration();
            //error[i] = info.getStdDeviation();
        }

        chart.addSeries(description, x, y);

        try {
            BitmapEncoder.saveBitmap(chart, "C:/Users/Jonas/Desktop/plots/load_count_workers_as_files", BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void compareLoadWithFixedFiles() {

        XYChart chart = new XYChartBuilder().xAxisTitle("workers").yAxisTitle("duration(avg)").build();


        addWorkerSeries("load_count_schema", chart, 5);
        addWorkerSeries("load_count_schema", chart, 11);
        addWorkerSeries("load_count_schema", chart, 20);

        int[] zero = {0};
        chart.addSeries("zero", zero, zero);

        try {
            BitmapEncoder.saveBitmap(chart, "C:/Users/Jonas/Desktop/plots/load_count_schema_comparison", BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void createArticlesPerAuthorChart(String logpath) {

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("C:/Users/Jonas/Desktop/articles_per_author_log.txt")));
            String line;
            int count = 1;
            ArrayList<Integer> artAvgVals = new ArrayList<>();
            ArrayList<Integer> artDeviationVals = new ArrayList<>();
            ArrayList<Integer> revAvgVals = new ArrayList<>();
            ArrayList<Integer> revDeviationVals = new ArrayList<>();
            ArrayList<Integer> xVals = new ArrayList<>();
            XYChart chart = new XYChartBuilder().xAxisTitle("file number").yAxisTitle("avg per author").build();

            while((line = reader.readLine()) != null) {
                if(line.startsWith("#"))
                    continue;

                String[] parts = line.split(" ");
                revAvgVals.add(Integer.parseInt(parts[0]));
                revDeviationVals.add(Integer.parseInt(parts[1]));
                artAvgVals.add(Integer.parseInt(parts[2]));
                artDeviationVals.add(Integer.parseInt(parts[3]));
                xVals.add(count++);

            }

            chart.addSeries("revisions", xVals, revAvgVals);
            chart.addSeries("articles", xVals, artAvgVals);

            BitmapEncoder.saveBitmap(chart, "C:/Users/Jonas/Desktop/art-rev-per-author", BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void createSeries(String filename, XYChart chart, String description) {

        DataContainer container = DataContainer.instance();
        container.loadInformation(filename);

        ArrayList<AppInformation> info = container.getWorkerDSSeries(description, 1, 16);
        ArrayList<Long> x = new ArrayList<>();
        ArrayList<Double> y = new ArrayList<>();
        ArrayList<Double> error = new ArrayList<>();

        for(AppInformation i : info) {
            x.add(i.getDataSize());
            y.add(i.getAvgDuration());
            error.add(i.getStdDeviation());
        }

        if(filename.endsWith("diff_log.txt")) {
            chart.addSeries("different", x, y, error);
        } else {
            chart.addSeries("same", x, y, error);
        }

    }

    public static void addZero(XYChart chart) {
        int[] zero = {0};
        chart.addSeries("zero", zero, zero);
    }

    public static void createSameVsDiffWorker() {

        //XYChart selectChart = new XYChartBuilder().title("select").xAxisTitle("#files").yAxisTitle("duration(avg)").build();
        //XYChart filterChart = new XYChartBuilder().title("filter").xAxisTitle("#files").yAxisTitle("duration(avg)").build();
        //XYChart groupByChart = new XYChartBuilder().title("groupBy").xAxisTitle("#files").yAxisTitle("duration(avg)").build();
        XYChart crossjoinChart = new XYChartBuilder().title("crossJoin").xAxisTitle("#files").yAxisTitle("duration(avg)").build();

        //addZero(selectChart);
        //addZero(filterChart);
        //addZero(groupByChart);
        addZero(crossjoinChart);


        //createSeries("C:/Users/Jonas/Desktop/perf_worker_executor_diff_log.txt", selectChart, "select_count");
        //createSeries("C:/Users/Jonas/Desktop/perf_worker_executor_same_log.txt", selectChart, "select_count");
        //createSeries("C:/Users/Jonas/Desktop/perf_worker_executor_diff_log.txt", filterChart, "filter_count");
        //createSeries("C:/Users/Jonas/Desktop/perf_worker_executor_same_log.txt", filterChart, "filter_count");
        //createSeries("C:/Users/Jonas/Desktop/perf_worker_executor_diff_log.txt", groupByChart, "groupby_count");
        //createSeries("C:/Users/Jonas/Desktop/perf_worker_executor_same_log.txt", groupByChart, "groupby_count");
        createSeries("C:/Users/Jonas/Desktop/perf_crossjoin_diff_log.txt", crossjoinChart, "crossjoin_count");
        createSeries("C:/Users/Jonas/Desktop/perf_crossjoin_same_log.txt", crossjoinChart, "crossjoin_count");

        try {
            //BitmapEncoder.saveBitmap(selectChart, "C:/Users/Jonas/Desktop/plots/same_diff_worker/select", BitmapEncoder.BitmapFormat.PNG);
            //BitmapEncoder.saveBitmap(filterChart, "C:/Users/Jonas/Desktop/plots/same_diff_worker/filter", BitmapEncoder.BitmapFormat.PNG);
            //BitmapEncoder.saveBitmap(groupByChart, "C:/Users/Jonas/Desktop/plots/same_diff_worker/groupBy", BitmapEncoder.BitmapFormat.PNG);
            BitmapEncoder.saveBitmap(crossjoinChart, "C:/Users/Jonas/Desktop/plots/same_diff_worker/crossJoin", BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void createCoreChart() {

        DataContainer container = DataContainer.instance();
        container.loadInformation("C:/Users/Jonas/Desktop/load_core_log.txt");
        XYChart chart = new XYChartBuilder().xAxisTitle("#cores").yAxisTitle("duration(avg)").build();
        addZero(chart);

        for(String description : container.getDescriptions()) {
            ArrayList<AppInformation> infoList = container.getDSCoreSeries(description, 1, 1);
            ArrayList<Integer> x = new ArrayList<>();
            ArrayList<Double> y = new ArrayList<>();
            ArrayList<Double> error = new ArrayList<>();

            for(AppInformation info : infoList) {
                x.add(info.getCores());
                y.add(info.getAvgDuration());
                error.add(info.getStdDeviation());
            }

            chart.addSeries(description, x, y, error);
        }

        try {
            BitmapEncoder.saveBitmap(chart, "C:/Users/Jonas/Desktop/plots/cores/load_compare_with_diff_cores", BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {

        createCoreChart();
        //createSameVsDiffWorker();
        //createArticlesPerAuthorChart("C:/Users/Jonas/Desktop/articles_per_author_log.txt");
        //compareLoadWithFixedFiles();
        //compareLoad();
        //compareSameWorkersAsFiles("load_count_schema");

    }

}
