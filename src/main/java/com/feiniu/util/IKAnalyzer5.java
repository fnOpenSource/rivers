package com.feiniu.util;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
 
public class IKAnalyzer5 extends Analyzer{
 
    private boolean useSmart;
     
    public boolean useSmart() {
        return useSmart;
    }
 
    public void setUseSmart(boolean useSmart) {
        this.useSmart = useSmart;
    }
 
    /**
     * IK分词器Lucene  Analyzer接口实现类
     * 
     * 默认细粒度切分算法
     */
    public IKAnalyzer5(){
        this(false);
    }
     
    /**
     * IK分词器Lucene Analyzer接口实现类
     * 
     * @param useSmart 当为true时，分词器进行智能切分
     */
    public IKAnalyzer5(boolean useSmart){
        super();
        this.useSmart = useSmart;
    }
  
    public static Analyzer getInstance(boolean useSmart){
    	return new IKAnalyzer5(useSmart);
    }
     
    /**
     * 重写最新版本的createComponents
     * 重载Analyzer接口，构造分词组件
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer _IKTokenizer = new IKTokenizer5(this.useSmart());
        return new TokenStreamComponents(_IKTokenizer);
    }
}