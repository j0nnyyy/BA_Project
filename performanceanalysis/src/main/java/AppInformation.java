public class AppInformation {

    private int duration, cores;
    private long dataSize;
    private double avgTimeC;
    private String description;

    public AppInformation(int duration, int cores, long dataSize, String description) {

        this.duration = duration;
        this.cores = cores;
        this.dataSize = dataSize;
        this.description = description;

    }

    public void setAvgTimeC(double avgTimeC) {
        this.avgTimeC = avgTimeC;
    }

    public int getDuration() {
        return duration;
    }

    public int getCores() {
        return cores;
    }

    public long getDataSize() {
        return dataSize;
    }

    public double getAvgTimeC() {
        return avgTimeC;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return cores + " " + dataSize + " " + duration + " " + avgTimeC + " " + description;
    }

}
