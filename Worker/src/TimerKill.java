
public class TimerKill extends Thread {
	private int count = 0;
	
	/**
	 * @param count
	 */
	public TimerKill(int count) {
		super();
		this.count = count;
	}

	/**
	 * @return the count
	 */
	public int getCount() {
		return count;
	}


	/**
	 * @param count the count to set
	 */
	public void setCount(int count) {
		this.count = count;
	}



	public void run() {
        try {
			Thread.sleep(this.count);
			AwsConsoleApp.killmePlease();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
}
