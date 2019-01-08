package mtime.mq.nsq;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author hongmiao.yu
 */
public class VersionTest {

    @Test
    public void get() throws Exception {
        Assert.assertEquals("1.0.10", Version.get());
    }

}
