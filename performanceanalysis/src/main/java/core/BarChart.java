package core;

import org.knowm.xchart.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class BarChart {

    public static void createAuthorTitleBarChart(String filepath, ArrayList<String> series) {

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filepath)));
            ArrayList<Integer> xVals = new ArrayList<>();
            ArrayList<Integer> authorVals = new ArrayList<>();
            ArrayList<Integer> titleVals = new ArrayList<>();
            ArrayList<Integer> revisionVals = new ArrayList<>();
            String line;
            CategoryChart chart = new CategoryChartBuilder().title("Author-title-revision-distribution")
                    .xAxisTitle("file number").yAxisTitle("count").build();

            while((line = reader.readLine()) != null) {
                if(line.startsWith("#"))
                    continue;
                String[] parts = line.split(" ");
                xVals.add(Integer.parseInt(parts[0]));
                authorVals.add(Integer.parseInt(parts[1]));
                titleVals.add(Integer.parseInt(parts[2]));
                revisionVals.add(Integer.parseInt(parts[3]));
            }

            if(series.contains("authors"))
                chart.addSeries("authors", xVals, authorVals);
            if(series.contains("titles"))
                chart.addSeries("titles", xVals, titleVals);
            if(series.contains("revisions"))
                chart.addSeries("revisions", xVals, revisionVals);

            BitmapEncoder.saveBitmap(chart, "C:/Users/Jonas/Desktop/author-title-revision-distribution.png", BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void createWholeAuthorTitleBarChart(String filepath, ArrayList<String> series) {

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filepath)));
            ArrayList<Integer> xVals = new ArrayList<>();
            ArrayList<Integer> authorVals = new ArrayList<>();
            ArrayList<Integer> titleVals = new ArrayList<>();
            ArrayList<Integer> revisionVals = new ArrayList<>();
            xVals.add(0);
            int author = 0;
            int title = 0;
            int revision = 0;
            String line;
            CategoryChart chart = new CategoryChartBuilder().title("Author-title-distribution")
                    .xAxisTitle("file number").yAxisTitle("count").build();

            while((line = reader.readLine()) != null) {
                if(line.startsWith("#"))
                    continue;
                String[] parts = line.split(" ");
                author += Integer.parseInt(parts[1]);
                title += Integer.parseInt(parts[2]);
                revision += Integer.parseInt(parts[3]);
            }

            authorVals.add(author);
            titleVals.add(title);
            revisionVals.add(revision);

            if(series.contains("authors"))
                chart.addSeries("authors", xVals, authorVals);
            if(series.contains("titles"))
                chart.addSeries("titles", xVals, titleVals);
            if(series.contains("revisions"))
                chart.addSeries("revisions", xVals, revisionVals);

            BitmapEncoder.saveBitmap(chart, "C:/Users/Jonas/Desktop/author-title-comparison.png", BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void createAvgDevChart(String filepath) {

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filepath)));
            ArrayList<Integer> xVals = new ArrayList<>();
            ArrayList<Integer> avgVals = new ArrayList<>();
            ArrayList<Integer> stddevVals = new ArrayList<>();
            int[] zero = {0};
            int count = 1;
            String line;
            XYChart chart = new XYChartBuilder().title("Revision-Average")
                    .xAxisTitle("file number").yAxisTitle("avg(revisions)").build();

            while((line = reader.readLine()) != null) {
                if(line.startsWith("#"))
                    continue;
                String[] parts = line.split(" ");
                xVals.add(count);
                avgVals.add(Integer.parseInt(parts[4]));
                stddevVals.add(Integer.parseInt(parts[5]));
                count++;
            }

            chart.addSeries("avg", xVals, avgVals, stddevVals);
            chart.addSeries("zero", zero, zero);

            BitmapEncoder.saveBitmap(chart, "C:/Users/Jonas/Desktop/revision-avg.png", BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        String[] series = {"authors", "titles", "revisions"};
        BarChart.createAuthorTitleBarChart("C:/Users/Jonas/Desktop/author_title_log.txt", new ArrayList<>(Arrays.asList(series)));
        //BarChart.createAvgDevChart("C:/Users/Jonas/Desktop/author_title_log.txt");
    }

}
