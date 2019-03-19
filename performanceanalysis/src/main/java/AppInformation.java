public class AppInformation {

    private int duration, cores;
    private long dataSize;
    private double avgTimeC;

    public AppInformation(int duration, int cores, long dataSize) {

        this.duration = duration;
        this.cores = cores;
        this.dataSize = dataSize;

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

    @Override
    public String toString() {
        return cores + " " + dataSize + " " + duration + " " + avgTimeC;
    }

}
