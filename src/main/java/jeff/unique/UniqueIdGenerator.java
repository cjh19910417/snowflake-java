package jeff.unique;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.eaio.uuid.UUIDGen;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;

/**
 * 基于Twitter的snowflake理论,添加MAC+PID的hash值作为workerId:
 * (a) id构成: 39位的时间前缀 + 16位的节点标识 + 9位的sequence避免并发的数字(9位不够用时强制得到新的时间前缀)
 * (b) 对系统时间的依赖性非常强，需关闭ntp的时间同步功能。当检测到ntp时间调整后，将会拒绝分配id
 *
 * @author Jeff.Chan
 */

public class UniqueIdGenerator {

    private final static Logger logger = LoggerFactory.getLogger(UniqueIdGenerator.class);
    /**
     * mac
     */
    private final static long clockSeqAndNode = UUIDGen.getClockSeqAndNode();
    /**
     * macPidHashCode的16个低位取值
     */
    private static final long workerId;
    /**
     * mac+pid的hashcode
     */
    private static final long macPidHashCode;

    /**
     * 时间起始标记点，作为基准，一般取系统第一次运行的的时间毫秒值作为"新纪元"
     */
    private final long epoch = 1451577600605L;
    /**
     * 毫秒内序列
     */
    private long sequence = 0L;
    /**
     * 毫秒内自增位数
     */
    private final long sequenceBits = 9L;
    /**
     * 毫秒内最大自增序列值:511,9位
     */
    private final long sequenceMax = -1L ^ -1L << this.sequenceBits;
    /**
     * worker标识位数
     */
    private final long workerIdBits = 16L;
    /**
     * workerId左移动位
     */
    private final long workerIdLeftShift = this.sequenceBits;
    /**
     * 时间戳左移动位
     */
    private final long timestampLeftShift = this.sequenceBits + this.workerIdBits;
    /**
     * 上次生产id时间戳
     */
    private long lastTimestamp = -1L;

    static {
        macPidHashCode = (clockSeqAndNode + "" + getJvmPid()).hashCode();
        workerId = macPidHashCode & 0xffff;//获取16个低位
    }

    private UniqueIdGenerator() {
    }

    /**
     * 获取uniqueId(long)
     *
     * @return id
     * @throws Exception
     */
    public synchronized long nextId() throws Exception {
        long timestamp = this.getCurrentTime();
        if (this.lastTimestamp == timestamp) { // 如果上一个timestamp与新产生的相等，则sequence加一(0-511循环); 对新的timestamp，sequence从0开始
            this.sequence = this.sequence + 1 & this.sequenceMax;
            if (this.sequence == 0) {
                timestamp = this.waitingNextMillis(this.lastTimestamp);// 重新生成timestamp
            }
        } else {
            this.sequence = 0;
        }

        if (timestamp < this.lastTimestamp) {
            logger.error(String.format("clock moved backwards.Refusing to generate id for %d milliseconds", (this.lastTimestamp - timestamp)));
            throw new Exception(String.format("clock moved backwards.Refusing to generate id for %d milliseconds", (this.lastTimestamp - timestamp)));
        }

        this.lastTimestamp = timestamp;
        return timestamp - this.epoch << this.timestampLeftShift | UniqueIdGenerator.workerId << this.workerIdLeftShift | this.sequence;
    }

    private static UniqueIdGenerator idGenerator = new UniqueIdGenerator();

    public static UniqueIdGenerator getInstance() {
        return idGenerator;
    }

    /**
     * 获取pid
     *
     * @return pid
     */
    private static String getJvmPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        //System.out.println(name);
        // get pid
        String pid = name.split("@")[0];
        System.out.println("Pid is:" + pid);
        return pid;
    }

    /**
     * 等待下一个毫秒的到来, 保证返回的毫秒数在参数lastTimestamp之后
     */
    private long waitingNextMillis(long lastTimestamp) {
        long timestamp = this.getCurrentTime();
        while (timestamp <= lastTimestamp) {
            timestamp = this.getCurrentTime();
        }
        return timestamp;
    }

    /**
     * 获得系统当前毫秒数
     */
    private long getCurrentTime() {
        return System.currentTimeMillis();
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Set<Long> set = new HashSet<>();
        for (int i = 0; i < 2000000; i++) {
            long nextId = UniqueIdGenerator.getInstance().nextId();
            if (set.contains(nextId)) {
                logger.error("有重复的!!!!!!!!!!!");
            } else {
                set.add(nextId);
            }
            //System.out.println(UniqueIdGenerator.getInstance().nextId());
        }
        System.out.println("Elapsed time : " + (System.currentTimeMillis() - start));
    }


}