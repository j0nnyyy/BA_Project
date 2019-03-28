package core;

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
                xValues[i] = infoList.get(i).getCores();
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
                        xValues[i] = infoList.get(i).getCores();
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

}
