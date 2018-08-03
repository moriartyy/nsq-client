package mtime.mq.nsq;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author hongmiao.yu
 */
public class Command {

    private static final Charset ASCII = Charset.forName("ascii");

    private String name;
    private List<String> arguments = new ArrayList<>();
    private List<byte[]> data = new ArrayList<>();
    private boolean responsive = false;

    Command() { /** no instances */}

    Command(Builder builder) {
        this.name = builder.name;
        this.arguments = builder.arguments;
        this.data = builder.data;
        this.responsive = builder.responsive;
    }

    public boolean isResponsive() {
        return this.responsive;
    }

    public String getLine() {
        StringBuilder line = new StringBuilder();
        line.append(name);
        for (String argument : arguments) {
            line.append(' ').append(argument);
        }
        return line.toString();
    }

    public List<byte[]> getData() {
        return data;
    }

    public String toString() {
        return this.getLine();
    }

    public static Builder instance(String name) {
        return new Builder(name);
    }

    /**
     * @author hongmiao.yu
     */
    public static class Builder {
        private String name;
        private List<String> arguments = new ArrayList<>();
        private List<byte[]> data = new ArrayList<>();
        private boolean responsive = false;

        Builder(String name) {
            this.name = name;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder addArgument(String argument) {
            this.arguments.add(argument);
            return this;
        }

        public Builder addArgument(String... arguments) {
            this.arguments.addAll(Arrays.asList(arguments));
            return this;
        }

        public Builder addArgument(byte[] argument) {
            return addArgument(new String(argument, ASCII));
        }

        public Builder addArgument(int argument) {
            return addArgument(String.valueOf(argument));
        }

        public Builder addArgument(long argument) {
            return addArgument(String.valueOf(argument));
        }

        public Builder data(byte[] data) {
            this.data.add(data);
            return this;
        }

        public Builder data(List<byte[]> data) {
            this.data.addAll(data);
            return this;
        }

        public Builder responsive(boolean responsive) {
            this.responsive = responsive;
            return this;
        }

        public Command build() {
            return new Command(this);
        }

    }
}
