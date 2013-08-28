package com.renren.redis.client;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import javax.imageio.ImageIO;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;

public class Util
{
	public static void main(String[] args)throws Exception
	{
		StringBuffer buf=new StringBuffer();
		
		char s[]={'a','b','c'};
		char s2[]=new char[s.length+3];
		s2[0]='d';s[1]='e';s[0]='f';
		
		ArrayList<String> list=new ArrayList<String>();
		list.add("b");
		list.add("a");
		
		System.out.println(System.getenv("file.encoding"));
	}
	
	private static DocumentBuilder doc_builder;//解析xml
	private static MessageDigest md;//生成md5
	static
	{
		try
		{
			DocumentBuilderFactory dbf=DocumentBuilderFactory.newInstance();
			doc_builder=dbf.newDocumentBuilder();
			md=MessageDigest.getInstance("md5");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static Document Parse(byte[] content)throws Exception
	{
		ByteArrayInputStream s=new ByteArrayInputStream(content);
		Document doc=doc_builder.parse(s);
		s.close();
		return doc;
	}
	
	public final static void Long2Bytes(long res,byte[] buf,int offset)
	{
		buf[offset] = (byte) (res & 0x0ff);
		buf[offset+1] = (byte) ((res >> 8) & 0xff);
		buf[offset+2] = (byte) ((res >> 16) & 0xff);
		buf[offset+3] = (byte) (res >> 24 & 0xff);
		buf[offset+4] = (byte) ((res>>24)>>8 & 0x0ff);
		buf[offset+5] = (byte) ((res>>24)>>16 & 0x0ff);
		buf[offset+6] = (byte) ((res>>24)>>24 & 0x0ff);
		buf[offset+7] = (byte) (((res>>24)>>24)>>>8 & 0xff);
	}
	public static final long Bytes2Long(byte[] res,int offset)
	{
		return (res[0+offset] & 0x0ffl)
				| ((res[1+offset] << 8) & 0x0ff00L) 
				| ((res[2+offset] <<16) & 0x0ff0000L )
				| ((res[3+offset]<< 24) & 0x00000000ff000000L)
				| (((long)res[4+offset]<< 32) & 0x000000ff00000000L)
				| (((long)(res[5+offset]<< 20) <<20) & 0x0000ff0000000000L)
				| ((((long)(res[6+offset]<< 20) << 20) << 8 ) & 0x00ff000000000000L)
				| (((((long)(res[7+offset]<< 20) << 20) << 16) ) & 0xff00000000000000L);
	}
	
	public static String Read(String file_name)throws IOException//一次读入全部文本
	{
		File f=new File(file_name);
		byte[] buf=new byte[(int)f.length()];
		DataInputStream in=new DataInputStream(new BufferedInputStream(new FileInputStream(file_name)));
		in.readFully(buf);
		in.close();
		return new String(buf,0,buf.length,"gbk");
	}
	public static void Write(String fileName,String content)throws IOException//一次写入文件
	{
		BufferedWriter file=new BufferedWriter(new FileWriter(fileName));
		file.write(content);
		file.close();
	}
	
	public static byte[] Read2(String file_name)throws IOException//一次读入全部二进数据制
	{
		File f=new File(file_name);
		byte[] buf=new byte[(int)f.length()];
		DataInputStream in=new DataInputStream(new BufferedInputStream(new FileInputStream(file_name)));
		in.readFully(buf);
		in.close();
		return buf;
	}
	public static BufferedReader Open(String file_name)throws IOException//读写打开文本/二进制文件
	{
		return new BufferedReader(new FileReader(file_name));
	}
	public static BufferedWriter Open2(String file_name)throws IOException
	{
		return new BufferedWriter(new FileWriter(file_name));
	}
	public static DataInputStream Open3(String file_name)throws FileNotFoundException
	{
		return new DataInputStream(new BufferedInputStream(new FileInputStream(file_name)));
	}
	public static DataOutputStream Open4(String file_name)throws FileNotFoundException
	{
		return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file_name)));
	}
	
	public static BufferedImage ReadImage(String file_name)throws IOException
	{
		BufferedImage image=ImageIO.read(new File(file_name));
		return image;
	}
	
	public static ThreadPoolExecutor GetThreadPool(int size)throws Exception
	{
		ThreadPoolExecutor poolExecutor=(ThreadPoolExecutor)Executors.newCachedThreadPool();
		poolExecutor.setCorePoolSize(size);
		poolExecutor.setMaximumPoolSize(size);
		poolExecutor.setKeepAliveTime(60*1000,TimeUnit.MILLISECONDS);
		poolExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		return poolExecutor;
	}
	
	public static boolean IsWinOs()
	{
		String s=System.getProperty("os.name");
		if(s.startsWith("Windows")) return true;
		return false;
	}
	
	public static void Delete(String path)
	{
		File f=new File(path);
		if(f.isFile()) f.delete();
		else if(f.isDirectory())
		{
			String[] files=f.list();
			for(int i=0;i<files.length;i++) Delete(path+"/"+files[i]);
			f.delete();
		}
		else throw new RuntimeException("unknown type "+path);
	}
	
	public static void Clear(String path)//清空指定的目录
	{
		File f=new File(path);
		if(f.isDirectory())
		{
			String[] files=f.list();
			for(int i=0;i<files.length;i++) Delete(path+"/"+files[i]);
		}
	}
	public static void Clear(String path,int days)throws Exception
	{
		Clear(path,days,true);
	}
	private static void Clear(String path,int days,boolean reserve)throws Exception
	{
		File f=new File(path);
		if(!f.exists()) return;
		
		long time=System.currentTimeMillis();
		long time2=f.lastModified();
		
		if(f.isFile())
		{
			if(time-time2>days*86400000) 
			{
				Util.Log("rm "+f.getCanonicalPath());
				f.delete();
			}
		}
		else
		{
			String[] files=f.list();
			for(int i=0;i<files.length;i++)
			{
				Clear(path+"/"+files[i],days,false);//不保留子目录
			}
			if(!reserve && time-time2>days*86400000) f.delete();
		}
	}
	
	public static int GetDiskUsePercent(String disk_info,String mount_dir)//获取指定磁盘分区的使用率
	{
		int r=-1;
		String[] s=disk_info.split("\n");
		for(int i=0;i<s.length;i++)
		{
			if(s[i].indexOf(mount_dir)==-1) continue;
			String[] t=s[i].split("\\s+");
			for(int j=0;j<t.length;j++)
			{
				if(t[j].indexOf("%")==-1) continue;
				String p=t[j].replaceAll("%","");
				r=(int)Float.parseFloat(p);
				return r;
			}
		}
		return r;
	}
	
	public static String Execute(String cmd)throws IOException,InterruptedException
	{
		String[] s=cmd.split("\\s+");
		ProcessBuilder pb=new ProcessBuilder(s);
		pb.redirectErrorStream(true);
		Process p=pb.start();
		byte[] buf=new byte[4096];
		int c=-1;
		InputStream in=p.getInputStream();
		StringBuffer sb=new StringBuffer();
		while((c=in.read(buf))!=-1)
		{
			String t=new String(buf,0,c);
			sb.append(t);
		}
		in.close();
		return sb.toString();
	}
	
	public static int System(String cmd)throws IOException,InterruptedException
	{
		String[] s=cmd.split("\\s+");
		ProcessBuilder pb=new ProcessBuilder(s);
		pb.redirectErrorStream(true);
		Process p=pb.start();
		byte[] buf=new byte[4096];
		int c=-1;
		InputStream in=p.getInputStream();
		while((c=in.read(buf))!=-1)
		{
			String t=new String(buf,0,c);
			System.out.print(t);
		}
		in.close();
		int r=p.waitFor();
		p.getOutputStream().close();
		p.getErrorStream().close();
		return r;
	}
	
	public static String Time()
	{
		SimpleDateFormat format=new SimpleDateFormat("yyMMdd_HHmmss");
		return format.format(new Date());
	}
	
	public static void Log(String s)
	{
		SimpleDateFormat format=new SimpleDateFormat("yy-MM-dd HH:mm:ss");
		String t=format.format(new Date());
		System.out.println(t+" "+s.trim());
	}
	
	public static byte[] Compress(byte[] input) { return Compress(input,0,input.length);}
	public static byte[] Compress(byte[] input,int offset,int length)
	{
	    Deflater compressor=new Deflater();
	    compressor.setLevel(Deflater.BEST_COMPRESSION);
	    compressor.setInput(input,offset,length);
	    compressor.finish();

	    ByteArrayOutputStream bos = new ByteArrayOutputStream(length);
	    try
	    {
	    	compressor.setLevel(Deflater.BEST_COMPRESSION);
	    	compressor.setInput(input);
	    	compressor.finish();

	    	byte[] buf = new byte[1024];
	    	while (!compressor.finished())
	    	{
	    		int count = compressor.deflate(buf);
	    		bos.write(buf, 0, count);
	    	}

	    }
	    finally
	    {      
	    	compressor.end();
	    }
	    return bos.toByteArray();
	}
	public static byte[] Uncompress(final byte[] input)throws Exception
	{
	  	ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);
	  	Inflater decompressor = new Inflater();
	  	decompressor.setInput(input);
	  	byte[] buf = new byte[1024];// Decompress the data
	  	while (!decompressor.finished())
	  	{
	  		int count = decompressor.inflate(buf);
	  		bos.write(buf, 0, count);
	  	}
	  	decompressor.end();
	  	return bos.toByteArray();
	}
	
	public static long MD5(String content)
	{
		long md5=0;
		try
		{
			md5=MD5(content.getBytes());
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return md5; 
	}
	public static long MD5(byte[] content)throws Exception
	{
		byte[] r=md.digest(content);
		long d=0;
		for(int i=7;i>=0;i--)
		{
			long l=r[i]&0xFF;
			d|=(l<<(8*i));
		}
		return d;
	}
}