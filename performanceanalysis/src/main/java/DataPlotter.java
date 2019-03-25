import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;

import java.util.ArrayList;
import java.util.Scanner;

public class DataPlotter {

    public static void plotInformation(InformationContainer container, String... descriptions) {

        for(String description : descriptions) {
            XYChart chart = new XYChartBuilder().xAxisTitle("#Nodes").yAxisTitle("Duration").build();

            for (ArrayList<ArrayList<AppInformation>> dsLists : container.coreAvgPerDescDs) {
                if (dsLists.get(0).get(0).getDescription().equals(description)) {
                    chart.setTitle(description);
                    for (ArrayList<AppInformation> infoList : dsLists) {
                        double[] xValues = new double[infoList.size()];
                        double[] yValues = new double[infoList.size()];
                        String seriesName = " ";

                        for (int i = 0; i < infoList.size(); i++) {
                            AppInformation info = infoList.get(i);

                            xValues[i] = info.getCores();
                            yValues[i] = info.getAvgTimeC();

                            seriesName = "" + info.getDataSize();
                        }

                        chart.addSeries(seriesName, xValues, yValues);
                    }
                }
            }

            new SwingWrapper(chart).displayChart();
        }


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
        String[] descriptions = new String[container.coreAvgPerDescDs.size()];

        for(int i = 0; i < container.coreAvgPerDescDs.size(); i++) {
            ArrayList<ArrayList<AppInformation>> tmp = container.coreAvgPerDescDs.get(i);
            descriptions[i] = tmp.get(0).get(0).getDescription();
        }

        System.out.println("Choose series to plot.");
        System.out.println("Possible series: ");

        for(int i = 0; i < descriptions.length; i++) {
            System.out.println(descriptions[i]);
        }

        Scanner scanner = new Scanner(System.in);
        String line = scanner.nextLine();

        String[] parts = line.split(" ");

        DataPlotter.plotInformation(container, parts);

        scanner.close();

    }

}
