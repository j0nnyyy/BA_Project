import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;

import java.util.ArrayList;

public class DataPlotter {

    public static void plotInformation(InformationContainer container) {

        XYChart chart = new XYChartBuilder().title("Core-Duration-Chart").xAxisTitle("#Cores").yAxisTitle("Duration").build();

        for(ArrayList<AppInformation> infoList : container.coreAvgPerDS) {
            double[] xValues = new double[infoList.size()];
            double[] yValues = new double[infoList.size()];
            String seriesName = " ";

            for(int i = 0; i < infoList.size(); i++) {
                AppInformation info = infoList.get(i);

                xValues[i] = info.getCores();
                yValues[i] = info.getAvgTimeC();

                seriesName = "" + info.getDataSize();
            }

            chart.addSeries(seriesName, xValues, yValues);
        }

        new SwingWrapper(chart).displayChart();

    }

    public static void main(String[] args) {

        String path = null;

        if(args.length == 1) {
            path = args[0];
        } else {
            System.err.println("Invalid argument count! Only one argument needed: filepath");
            return;
        }

        InformationContainer container = new InformationContainer(path);
        DataPlotter.plotInformation(container);

    }

}
