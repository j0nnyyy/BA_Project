package core;

import java.util.ArrayList;

public class AppInformation {

    private int workers, cores;
    private ArrayList<Double> durations = new ArrayList<>();
    private long dataSize;
    private String description;

    public AppInformation(int workers, int cores, long dataSize, String description) {

        this.workers = workers;
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

    public double getStdDeviation() {

        double avg = getAvgDuration();
        double sum = 0;

        for(int i = 0; i < durations.size(); i++) {
            double deviation = Math.pow(avg - durations.get(i), 2);
            sum += deviation;
        }

        sum /= durations.size() - 1;
        sum = Math.sqrt(sum);

        return sum;

    }

    public double getStdError() {

        double avg = getAvgDuration();
        double sum = 0;

        for(int i = 0; i < durations.size(); i++) {
            double deviation = Math.pow(avg - durations.get(i), 2);
            sum += deviation;
        }

        sum /= durations.size() - 1;
        sum = Math.sqrt(sum);

        sum /= Math.sqrt(durations.size());

        return sum;

    }

    public ArrayList<Double> getDurations() {
        return durations;
    }

    public int getWorkers() {
        return workers;
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
