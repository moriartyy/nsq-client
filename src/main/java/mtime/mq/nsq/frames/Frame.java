package mtime.mq.nsq.frames;

public abstract class Frame {

	private static final int RESPONSE = 0;
	private static final int ERROR = 1;
	private static final int MESSAGE = 2;

	private int size;
	private byte[] data;

	public static Frame instance(int type) {
		switch (type) {
		case RESPONSE :
			return new ResponseFrame();
		case ERROR :
			return new ErrorFrame();
		case MESSAGE :
			return new MessageFrame();
		}
		return null;
	}

	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public byte[] getData() {
		return data;
	}
	public void setData(byte[] data) {
		this.data = data;
	}
	
	
}
