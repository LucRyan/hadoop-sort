package com.ryan.hadoop.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable,
        WritableComparable<CompositeKeyWritable> {

    private long num;
    private String text;

    public CompositeKeyWritable() {
    }

    public CompositeKeyWritable(long num, String text) {
        this.num = num;
        this.text = text;
    }

    @Override
    public String toString() {
        return (new StringBuilder().append(text)).toString();
    }

    public void readFields(DataInput dataInput) throws IOException {
        text = WritableUtils.readString(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, text);
    }

    public int compareTo(CompositeKeyWritable objKeyPair) {

        return text.compareTo(objKeyPair.text);
    }

    public long getNum() {
        return num;
    }

    public void setNum(long num) {
        this.num = num;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}