/*
 * GNU GENERAL PUBLIC LICENSE.
 */
package com.sliva.plotter;

import java.util.Objects;

/**
 *
 * @author Sliva Co
 */
public class PlotterParams {

    private final String name;
    private final String tmpDrive;
    private final String tmp2Drive;

    public PlotterParams(String name, String tmpDrive, String tmp2Drive) {
        this.name = name;
        this.tmpDrive = tmpDrive;
        this.tmp2Drive = tmp2Drive;
    }

    public String getName() {
        return name;
    }

    public String getTmpDrive() {
        return tmpDrive;
    }

    public String getTmp2Drive() {
        return tmp2Drive;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + Objects.hashCode(this.name);
        hash = 41 * hash + Objects.hashCode(this.tmpDrive);
        hash = 41 * hash + Objects.hashCode(this.tmp2Drive);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PlotterParams other = (PlotterParams) obj;
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.tmpDrive, other.tmpDrive)) {
            return false;
        }
        if (!Objects.equals(this.tmp2Drive, other.tmp2Drive)) {
            return false;
        }
        return true;
    }

}
