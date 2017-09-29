package com.feiniu.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;

import java.util.HashMap;
import java.util.Map;

public final class FNPerFieldAnalyzerWrapper extends AnalyzerWrapper {
  private final Analyzer defaultAnalyzer;
  private final Map<String, Analyzer> fieldAnalyzers;

  public FNPerFieldAnalyzerWrapper(Analyzer defaultAnalyzer) {
    this(defaultAnalyzer, null);
  }

  public FNPerFieldAnalyzerWrapper(Analyzer defaultAnalyzer,
      Map<String, Analyzer> fieldAnalyzers) {
	super(Analyzer.PER_FIELD_REUSE_STRATEGY);
    this.defaultAnalyzer = defaultAnalyzer;
    this.fieldAnalyzers = (fieldAnalyzers != null) ? fieldAnalyzers : new HashMap<String, Analyzer>();
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    Analyzer analyzer = fieldAnalyzers.get(fieldName);
    return (analyzer != null) ? analyzer : defaultAnalyzer;
  }

  @Override
  protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
    return components;
  }
  
  @Override
  public String toString() {
    return "ECPerFieldAnalyzerWrapper(" + fieldAnalyzers + ", default=" + defaultAnalyzer + ")";
  }
  
  public void addAnalyzer(String field, Analyzer analyzer){
	  if (field != null && analyzer != null)
		  fieldAnalyzers.put(field, analyzer);
  }
}

