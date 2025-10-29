package com.testpocmaven;

import org.testng.annotations.*;
import com.pocmaven.ConcatenateValues;

public class TestConcatenateValues {

    @Test
    public void testConcatenateValue() {

        ConcatenateValues c1 = new ConcatenateValues();
        c1.concatFnameLname("manju", " kaushik");
    }

    @Test
    public void testFnameOnly() {

        ConcatenateValues c1 = new ConcatenateValues();
        c1.concatFnameLname("manju", "");
    }

    @Test
    public void testNovalues() {

        ConcatenateValues c1 = new ConcatenateValues();
        c1.concatFnameLname("", "");
    }
}
