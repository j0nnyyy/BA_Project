package core.containers;

import core.AppInformation;

public class CoreContainer {

    private AppInformation info;
    private int coreCount;

    public CoreContainer(int coreCount) {
        this.coreCount = coreCount;
    }

    public boolean add(AppInformation info) {

        if(info.getCores() == coreCount) {
            this.info = info;
            return true;
        }

        return false;

    }

    public AppInformation getInformation() {

        return info;

    }

    public void print() {

        System.out.println("            " + coreCount);

    }

    public int getCoreCount() {
        return coreCount;
    }

}
