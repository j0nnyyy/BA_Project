package core;

import core.containers.DataContainer;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;

import java.util.ArrayList;

public class DataPlotter {

    public static XYChart createChart(InformationContainer container, String description, String aggregation) {

        XYChart chart = new XYChartBuilder().xAxisTitle("#Nodes").yAxisTitle("Duration").build();

        ArrayList<ArrayList<AppInformation>> dsList = container.getSeriesByDescription(description);

        for(ArrayList<AppInformation> infoList : dsList) {
            double[] xValues = new double[infoList.size()];
            double[] yValues = new double[infoList.size()];
            long dataSize = infoList.get(0).getDataSize();

            for (int i = 0; i < xValues.length; i++) {
                xValues[i] = infoList.get(i).getWorkers();
            }

            switch (aggregation) {
                case "avg":
                    for (int i = 0; i < yValues.length; i++) {
                        yValues[i] = infoList.get(i).getAvgDuration();
                    }
                    break;
                case "min":
                    for (int i = 0; i < yValues.length; i++) {
                        yValues[i] = infoList.get(i).getMinDuration();
                    }
                    break;
                case "max":
                    for (int i = 0; i < yValues.length; i++) {
                        yValues[i] = infoList.get(i).getMaxDuration();
                    }
                    break;
            }

            chart.addSeries(dataSize + "", xValues, yValues);
        }

        return chart;

    }

    public static XYChart createChart(InformationContainer container, ArrayList<String> descriptions, String aggregation, long dataSize) {

        XYChart chart = new XYChartBuilder().xAxisTitle("#Nodes").yAxisTitle("Duration").build();

        for(String description : descriptions) {
            ArrayList<ArrayList<AppInformation>> dsList = container.getSeriesByDescription(description);

            for(ArrayList<AppInformation> infoList : dsList) {
                if(infoList.get(0).getDataSize() == dataSize) {
                    double[] xValues = new double[infoList.size()];
                    double[] yValues = new double[infoList.size()];

                    for (int i = 0; i < xValues.length; i++) {
                        xValues[i] = infoList.get(i).getWorkers();
                    }

                    switch (aggregation) {
                        case "avg":
                            for (int i = 0; i < yValues.length; i++) {
                                yValues[i] = infoList.get(i).getAvgDuration();
                            }
                            break;
                        case "min":
                            for (int i = 0; i < yValues.length; i++) {
                                yValues[i] = infoList.get(i).getMinDuration();
                            }
                            break;
                        case "max":
                            for (int i = 0; i < yValues.length; i++) {
                                yValues[i] = infoList.get(i).getMaxDuration();
                            }
                            break;
                    }

                    chart.addSeries(description, xValues, yValues);

                    break;
                }
            }
        }

        return chart;

    }

    public static XYChart createChart(DataContainer container,
                                      ArrayList<String> descriptions,
                                      ArrayList<Long> dataSizes,
                                      ArrayList<Integer> workers,
                                      int cores,
                                      Axis xAxisValue,
                                      String aggregation) {

        switch (xAxisValue) {
            case WORKERS:
                return createWorkerChart(container, descriptions, dataSizes, cores, aggregation);
            case CORES:
                return createCoreChart(container, descriptions, dataSizes, 1, aggregation);
            case DATASIZE:
                return createDataSizeChart(container, descriptions, workers, cores, aggregation);
        }

        return null;

    }

    private static XYChart createWorkerChart(DataContainer container,
                                      ArrayList<String> descriptions,
                                      ArrayList<Long> dataSizes,
                                      int cores,
                                      String aggregation) {

        XYChart chart = new XYChartBuilder().yAxisTitle("duration(" + aggregation + ")").xAxisTitle("#workers").build();

        double[] xValues;
        double[] yValues;

        if(descriptions.size() == 1) {
            for(Long dataSize : dataSizes) {
                ArrayList<AppInformation> infoList = container.getDSWorkerSeries(descriptions.get(0), dataSize, cores);

                if(infoList == null)
                    continue;

                xValues = new double[infoList.size()];
                yValues = new double[infoList.size()];

                for(int i = 0; i < infoList.size(); i++) {
                    switch (aggregation) {
                        case "avg":
                            yValues[i] = infoList.get(i).getAvgDuration();
                            break;
                        case "max":
                            yValues[i] = infoList.get(i).getMaxDuration();
                            break;
                        case "min":
                            yValues[i] = infoList.get(i).getMinDuration();
                            break;
                    }
                    xValues[i] = infoList.get(i).getWorkers();
                }

                chart.addSeries(dataSize.toString(), xValues, yValues);
            }
        } else {
            for (String description : descriptions) {
                ArrayList<AppInformation> infoList = container.getDSWorkerSeries(description, dataSizes.get(0), cores);

                if(infoList == null)
                    continue;

                xValues = new double[infoList.size()];
                yValues = new double[infoList.size()];

                for (int i = 0; i < infoList.size(); i++) {
                    switch (aggregation) {
                        case "avg":
                            yValues[i] = infoList.get(i).getAvgDuration();
                            break;
                        case "max":
                            yValues[i] = infoList.get(i).getMaxDuration();
                            break;
                        case "min":
                            yValues[i] = infoList.get(i).getMinDuration();
                            break;
                    }
                    xValues[i] = infoList.get(i).getWorkers();
                }

                chart.addSeries(description, xValues, yValues);
            }
        }

        return chart;

    }

    private static XYChart createDataSizeChart(DataContainer container,
                                        ArrayList<String> descriptions,
                                        ArrayList<Integer> workers,
                                        int cores,
                                        String aggregation) {

        XYChart chart = new XYChartBuilder().yAxisTitle("duration(" + aggregation + ")").xAxisTitle("datasize").build();

        double[] xValues;
        double[] yValues;

        if(descriptions.size() == 1) {
            for(Integer worker : workers) {
                ArrayList<AppInformation> infoList = container.getWorkerDSSeries(descriptions.get(0), worker, cores);

                if(infoList == null)
                    continue;

                xValues = new double[infoList.size()];
                yValues = new double[infoList.size()];

                for(int j = 0; j < infoList.size(); j++) {
                    switch (aggregation) {
                        case "avg":
                            yValues[j] = infoList.get(j).getAvgDuration();
                            break;
                        case "max":
                            yValues[j] = infoList.get(j).getMaxDuration();
                            break;
                        case "min":
                            yValues[j] = infoList.get(j).getMinDuration();
                            break;
                    }
                    xValues[j] = infoList.get(j).getDataSize();
                }

                chart.addSeries(worker.toString(), xValues, yValues);
            }
        } else {
            for (String description : descriptions) {
                ArrayList<AppInformation> infoList = container.getDSWorkerSeries(description, workers.get(0), cores);

                if(infoList == null)
                    continue;

                xValues = new double[infoList.size()];
                yValues = new double[infoList.size()];

                for (int i = 0; i < infoList.size(); i++) {
                    switch (aggregation) {
                        case "avg":
                            yValues[i] = infoList.get(i).getAvgDuration();
                            break;
                        case "max":
                            yValues[i] = infoList.get(i).getMaxDuration();
                            break;
                        case "min":
                            yValues[i] = infoList.get(i).getMinDuration();
                            break;
                    }
                    xValues[i] = infoList.get(i).getDataSize();
                }

                chart.addSeries(description, xValues, yValues);
            }
        }

        return chart;

    }

    private static XYChart createCoreChart(DataContainer container,
                                    ArrayList<String> descriptions,
                                    ArrayList<Long> dataSizes,
                                    int workers,
                                    String aggregation) {

        XYChart chart = new XYChartBuilder().yAxisTitle("duration(" + aggregation + ")").xAxisTitle("#cores").build();

        double[] xValues;
        double[] yValues;

        if(descriptions.size() == 1) {
            for(Long dataSize : dataSizes) {
                ArrayList<AppInformation> infoList = container.getDSCoreSeries(descriptions.get(0), dataSize, workers);

                if(infoList == null)
                    continue;

                xValues = new double[infoList.size()];
                yValues = new double[infoList.size()];

                for(int j = 0; j < infoList.size(); j++) {
                    switch (aggregation) {
                        case "avg":
                            yValues[j] = infoList.get(j).getAvgDuration();
                            break;
                        case "max":
                            yValues[j] = infoList.get(j).getMaxDuration();
                            break;
                        case "min":
                            yValues[j] = infoList.get(j).getMinDuration();
                            break;
                    }
                    xValues[j] = infoList.get(j).getCores();
                }

                chart.addSeries(dataSize.toString(), xValues, yValues);
            }
        } else {
            for (String description : descriptions) {
                ArrayList<AppInformation> infoList = container.getDSCoreSeries(description, dataSizes.get(0), workers);

                if(infoList == null)
                    continue;

                xValues = new double[infoList.size()];
                yValues = new double[infoList.size()];

                for (int i = 0; i < infoList.size(); i++) {
                    switch (aggregation) {
                        case "avg":
                            yValues[i] = infoList.get(i).getAvgDuration();
                            break;
                        case "max":
                            yValues[i] = infoList.get(i).getMaxDuration();
                            break;
                        case "min":
                            yValues[i] = infoList.get(i).getMinDuration();
                            break;
                    }
                    xValues[i] = infoList.get(i).getCores();
                }

                chart.addSeries(description, xValues, yValues);
            }
        }

        return chart;

    }

}
