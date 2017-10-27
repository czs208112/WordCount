package com.demo.proj.GroupByDemo.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Stbprp implements WritableComparable<Stbprp> {
	private int addvcd;
	private String stcd;
	private String stnm;
	private double lgtd;
	private double lttd;
	private String stlc;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(addvcd);
		out.writeUTF(stcd);
		out.writeUTF(stnm);
		out.writeDouble(lgtd);
		out.writeDouble(lttd);
		out.writeUTF(stlc);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.addvcd = in.readInt();
		this.stcd = in.readUTF();
		this.stnm = in.readUTF();
		this.lgtd = in.readDouble();
		this.lttd = in.readDouble();
		this.stlc = in.readUTF();
	}

	@Override
	public int compareTo(Stbprp o) {
		return this.addvcd - o.addvcd;
	}

	public int getAddvcd() {
		return addvcd;
	}

	public void setAddvcd(int addvcd) {
		this.addvcd = addvcd;
	}

	public String getStcd() {
		return stcd;
	}

	public void setStcd(String stcd) {
		this.stcd = stcd;
	}

	public String getStnm() {
		return stnm;
	}

	public void setStnm(String stnm) {
		this.stnm = stnm;
	}

	public double getLgtd() {
		return lgtd;
	}

	public void setLgtd(double lgtd) {
		this.lgtd = lgtd;
	}

	public double getLttd() {
		return lttd;
	}

	public void setLttd(double lttd) {
		this.lttd = lttd;
	}

	public String getStlc() {
		return stlc;
	}

	public void setStlc(String stlc) {
		this.stlc = stlc;
	}
}
