package szaqal;

import java.util.Arrays;
import java.util.BitSet;

/**
 *  BitSet简介

	 类实现了一个按需增长的位向量。位 set 的每个组件都有一个boolean值。用非负的整数将BitSet的位编入索引。
	 可以对每个编入索引的位进行测试、设置或者清除。通过逻辑与、逻辑或和逻辑异或操作，可以使用一个BitSet修改另一个BitSet的内容。

	 默认情况下，set 中所有位的初始值都是false。

	 每个位 set 都有一个当前大小，也就是该位 set 当前所用空间的位数。注意，这个大小与位 set 的实现有关，所以它可能随实现的不同而更改。
	 位 set 的长度与位 set 的逻辑长度有关，并且是与实现无关而定义的。

	 除非另行说明，否则将 null 参数传递给BitSet中的任何方法都将导致NullPointerException。

	 在没有外部同步的情况下，多个线程操作一个BitSet是不安全的
	 基本原理

	 BitSet是位操作的对象，值只有0或1即false和true，内部维护了一个long数组，初始只有一个long，所以BitSet最小的size是64，当随着存储的元素越来越多，
	 BitSet内部会动态扩充，最终内部是由N个long来存储，这些针对操作都是透明的。

	 用1位来表示一个数据是否出现过，0为没有出现过，1表示出现过。使用用的时候既可根据某一个是否为0表示，此数是否出现过。

	 一个1G的空间，有 8*1024*1024*1024=8.58*10^9bit，也就是可以表示85亿个不同的数
	 使用场景

	 常见的应用是那些需要对海量数据进行一些统计工作的时候，比如日志分析、用户数统计等等

	 如统计40亿个数据中没有出现的数据，将40亿个不同数据进行排序等。
	 现在有1千万个随机数，随机数的范围在1到1亿之间。现在要求写出一种算法，将1到1亿之间没有在随机数中的数求出来

 	******************************************************************************************************************
 *  总结:  BitSet的作用为(1)可以对数字去重； （2）也可以对数字排序;
 *  	  (1) 对于0~64之间的数字(>>6)，在set进去之后 >>6位之后的结果为0 ;
 *  	  所以这样set进去的值都是放在words[0]中的放的时候是以这些数字中1的位置来索引这些数字的(见set方法中的words[wordIndex] |= (1L << bitIndex);),
 *  	  在nextSetBit方法中再通过long word = words[u] & (WORD_MASK << fromIndex)和(u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word)取到这些数字的实际值;
 *  	  (2) 对于其他数字，在set进去之后会被存放在words[]中通过set方法中的 words[wordIndex] |= (1L << bitIndex)来存，
 *  	  然后通过long word = words[u] & (WORD_MASK << fromIndex)和(u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word)取到这些数字的实际值
 */
public class BitSetDemo {

	/**
	 * 求一个字符串包含的char
	 * 
	 */
	public static void containChars(String str) {
		BitSet used = new BitSet();
		for (int i = 0; i < str.length(); i++)
			used.set(str.charAt(i)); // set bit for char

		StringBuilder sb = new StringBuilder();
		sb.append("[");
		int size = used.size();
		System.out.println(size);
		for (int i = 0; i < size; i++) {
			if (used.get(i)) {
				sb.append((char) i);
			}
		}
		sb.append("]");
		System.out.println(sb.toString());
	}

	/**
	 * 求素数 有无限个。一个大于1的自然数，如果除了1和它本身外，不能被其他自然数整除(除0以外）的数称之为素数(质数） 否则称为合数
	 */
	public static void computePrime() {
		BitSet sieve = new BitSet(1024);//2^8 = 256    2^10=1024
		int size = sieve.size();//1024
		for (int i = 2; i < size; i++)//先把所有的都放在bitset里面
			sieve.set(i);
		int finalBit = (int) Math.sqrt(sieve.size());

		for (int i = 2; i < finalBit; i++)
			if (sieve.get(i))
				//遍历从1到sqrt(1024)内的所有数，对这些数以其2倍为起始值，然后以该数为步长得到的在bitset的size以内的数从bitset中清理掉
				//如i=2时 j取值为4到1024  j的值每次变化为 j+2      2的倍数
				//i=3时   j取值为6到1024  j每次变化为 j+3			  3的倍数
				//i=4时   4的倍数          .......... 一直到sqrt(1024)的倍数
				for (int j = 2 * i; j < size; j += i)
					sieve.clear(j);

		int counter = 0;
		for (int i = 1; i < size; i++) {
			if (sieve.get(i)) {
				System.out.printf("%5d", i);
				if (++counter % 15 == 0)
					System.out.println();
			}
		}
		System.out.println();
	}
	
	/**
	 * 进行数字排序
	 */
	public static void sortArray() {
		int[] array = new int[] { 423, 700, 9999, 2323, 356, 6400, 1,2,3,2,2,2,2 };
		NewBitSet bitSet = new NewBitSet(2 << 13);
		/**
		 * private long[] words;
		 *
		 * Creates a bit set whose initial size is large enough to explicitly
		 * represent bits with indices in the range {@code 0} through
		 * {@code nbits-1}. All bits are initially {@code false}.
		 *
		 * @param  nbits the initial size of the bit set
		 * @throws NegativeArraySizeException if the specified initial size
		 *         is negative

			public BitSet(int nbits) {
				// nbits can't be negative; size 0 is OK
				if (nbits < 0)
				throw new NegativeArraySizeException("nbits < 0: " + nbits);

				initWords(nbits);
				sizeIsSticky = true;
			}

			private void initWords(int nbits) {
				words = new long[wordIndex(nbits-1) + 1];
			}

		*
		 * Given a bit index, return word index containing it.
		 *
			private static int wordIndex(int bitIndex) {
				return bitIndex >> 6;
			}

			words的大小为   ((2<<13)-1)>>6+1

		 	然后size的大小为:
			public int size() {
				return words.length * (1<<6);  //(((2<<13)-1)>>6+1) * (1<<6)
			}

		 */

		// 虽然可以自动扩容，但尽量在构造时指定估算大小,默认为64
		System.out.println("BitSet size: " + bitSet.size());

		for (int i = 0; i < array.length; i++) {
			bitSet.set(array[i]);
		}
		//剔除重复数字后的元素个数
		int bitLen=bitSet.cardinality();	

		//进行排序，即把bit为true的元素复制到另一个数组
		int[] orderedArray = new int[bitLen];
		int k = 0;
		for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
			orderedArray[k++] = i;
		}

		System.out.println("After ordering: ");
		for (int i = 0; i < bitLen; i++) {
			System.out.print(orderedArray[i] + "\t");
		}
		
		System.out.println("iterate over the true bits in a BitSet");
		//或直接迭代BitSet中bit为true的元素iterate over the true bits in a BitSet
		for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
			System.out.print(i+"\t");
		}
		System.out.println("---------------------------");
	}
	
	/**
	 * 将BitSet对象转化为ByteArray
	 * @param bitSet
	 * @return
	 */
	public static byte[] bitSet2ByteArray(BitSet bitSet) {
        byte[] bytes = new byte[bitSet.size() / 8];
        for (int i = 0; i < bitSet.size(); i++) {
            int index = i / 8;
            int offset = 7 - i % 8;
            bytes[index] |= (bitSet.get(i) ? 1 : 0) << offset;
        }
        return bytes;
    }
 
	/**
	 * 将ByteArray对象转化为BitSet
	 * @param bytes
	 * @return
	 */
    public static BitSet byteArray2BitSet(byte[] bytes) {
        BitSet bitSet = new BitSet(bytes.length * 8);
        int index = 0;
        for (int i = 0; i < bytes.length; i++) {
            for (int j = 7; j >= 0; j--) {
                bitSet.set(index++, (bytes[i] & (1 << j)) >> j == 1 ? true
                        : false);
            }
        }
        return bitSet;
    }
	
	/**
	 * 简单使用示例
	 */
	public static void simpleExample() {
		String names[] = { "Java", "Source", "and", "Support" };
		BitSet bits = new BitSet();
		for (int i = 0, n = names.length; i < n; i++) {
			if ((names[i].length() % 2) == 0) {
				bits.set(i);
			}
		}

		System.out.println(bits);
		System.out.println("Size : " + bits.size());
		System.out.println("Length: " + bits.length());
		for (int i = 0, n = names.length; i < n; i++) {
			if (!bits.get(i)) {
				System.out.println(names[i] + " is odd");
			}
		}
		BitSet bites = new BitSet();
		bites.set(0);
		bites.set(1);
		bites.set(2);
		bites.set(3);
		bites.set(4);
		bites.andNot(bits);
		System.out.println(bites);
	}

	public static void main2(String[] args) {
		//BitSetDemo.computePrime();
//		BitSetDemo.sortArray();
//		NewBitSet bitSet = new NewBitSet(10);
//		System.out.println(bitSet.size());
//
//		for(int i=0;i<10;i++){
//			bitSet.set(i);
//
//			bitSet.get(i);
//		}

	}

	public static void main(String args[]) {
		//BitSet使用示例
//		BitSetDemo.containChars("How do you do? 你好呀");
//		BitSetDemo.computePrime();
//		BitSetDemo.sortArray();
//		BitSetDemo.simpleExample();
		
		
		//BitSet与Byte数组互转示例
		BitSet bitSet = new BitSet();
        bitSet.set(3, true);
        bitSet.set(98, true);
        System.out.println(bitSet.size()+","+bitSet.cardinality());
        //将BitSet对象转成byte数组
        byte[] bytes = BitSetDemo.bitSet2ByteArray(bitSet);
        System.out.println(Arrays.toString(bytes));

        //在将byte数组转回来
        bitSet = BitSetDemo.byteArray2BitSet(bytes);
        System.out.println(bitSet.size()+","+bitSet.cardinality());
        System.out.println(bitSet.get(3));
        System.out.println(bitSet.get(98));
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
			System.out.print(i+"\t");
		}
	}
}