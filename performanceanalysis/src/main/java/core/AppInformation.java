package core;

import java.util.ArrayList;

public class AppInformation {

    private int cores;
    private ArrayList<Double> durations = new ArrayList<>();
    private long dataSize;
    private String description;

    public AppInformation(int cores, long dataSize, String description) {

        this.cores = cores;
        this.dataSize = dataSize;
        this.description = description;

    }

    public void addDuration(double duration) {
        durations.add(duration);
    }

    public double getMinDuration() {

        double min = durations.get(0);

        for(int i = 1; i < durations.size(); i++) {
            if(durations.get(i) < min)
                min = durations.get(i);
        }

        return min;

    }

    public double getMaxDuration() {

        double max = durations.get(0);

        for(int i = 1; i < durations.size(); i++) {
            if(durations.get(i) > max)
                max = durations.get(i);
        }

        return max;

    }

    public double getAvgDuration() {

        double sum = 0;
        int count = durations.size();

        for(Double d : durations) {
            sum += d;
        }

        return sum / count;

    }

    public ArrayList<Double> getDurations() {
        return durations;
    }

    public int getCores() {
        return cores;
    }

    public long getDataSize() {
        return dataSize;
    }

    public String getDescription() {
        return description;
    }

}
