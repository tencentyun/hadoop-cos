package org.apache.hadoop.fs.cosn.ranger.security.sts;

public class GetSTSResponse {
    private String tempAK;
    private String tempSK;
    private String tempToken;

    public String getTempAK() {
        return tempAK;
    }

    public void setTempAK(String tempAK) {
        this.tempAK = tempAK;
    }

    public String getTempSK() {
        return tempSK;
    }

    public void setTempSK(String tempSK) {
        this.tempSK = tempSK;
    }

    public String getTempToken() {
        return tempToken;
    }

    public void setTempToken(String tempToken) {
        this.tempToken = tempToken;
    }
}