package com.github.piyushpatel2005.pluralsight.basics;

import org.joda.time.Instant;
import java.io.Serializable;

public class EnergyConsumption implements Serializable {
    private static final String[] FILE_HEADERS = {
            "Datetime", "AEP_MW"
    };

    private Instant datetime;
    private Double energyConsumption;

    public Instant getDatetime() {
        return datetime;
    }

    public void setDatetime(Instant datetime) {
        this.datetime = datetime;
    }

    public Double getEnergyConsumption() {
        return energyConsumption;
    }

    public void setEnergyConsumption(Double energyConsumption) {
        this.energyConsumption = energyConsumption;
    }

    public String asCSVRow(String delimiter) {
        return String.join(delimiter,
                this.datetime.toString(), this.energyConsumption.toString());
    }

    public static String getCSVHeader() {
        return String.join(",", "Datetime", "MW");
    }
}
