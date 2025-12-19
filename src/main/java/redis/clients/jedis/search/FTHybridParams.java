package redis.clients.jedis.search;

import static redis.clients.jedis.search.SearchProtocol.SearchKeyword.*;

import java.util.*;

import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.params.IParams;

/**
 * 参数构造器，用于 {@code FT.HYBRID} 组合文本检索与向量相似度查询。
 */
public class FTHybridParams implements IParams {

  private String searchQuery;
  private final List<Object> searchOptions = new ArrayList<>();

  private String vectorField;
  private String vectorParam;
  private Object vectorBlob;
  private final List<Object> vsimOptions = new ArrayList<>();
  // VSIM 预过滤策略：POLICY [ADHOC_BF|BATCHES] [BATCH_SIZE x]
  private Object policyKeyword;
  private Integer policyBatchSize;

  private final List<Object> combineOptions = new ArrayList<>();

  private int[] limit;
  private final List<Object> sortBy = new ArrayList<>();
  private boolean noSort;

  private boolean loadAll;
  private final List<Object> loadFields = new ArrayList<>();
  private Map<String, Boolean> returnFieldDecodeMap;

  private final List<Object> groupBy = new ArrayList<>();
  private final List<Object> apply = new ArrayList<>();
  private final List<String> filters = new ArrayList<>();

  private Map<String, Object> params;
  private Long timeout;
  private Integer dialect;

  private final List<Object> extraArgs = new ArrayList<>();

  public FTHybridParams search(String query) {
    this.searchQuery = query;
    return this;
  }

  public FTHybridParams searchOptions(Object... options) {
    Collections.addAll(this.searchOptions, options);
    return this;
  }

  public FTHybridParams searchScorer(String scorer, Object... scorerArgs) {
    if (scorer == null) {
      return this;
    }
    searchOptions.add(SCORER);
    if (scorerArgs != null && scorerArgs.length > 0) {
      searchOptions.add(1 + scorerArgs.length);
      searchOptions.add(scorer);
      Collections.addAll(searchOptions, scorerArgs);
    } else {
      searchOptions.add(scorer);
    }
    return this;
  }

  public FTHybridParams searchScoreAlias(String alias) {
    if (alias != null) {
      searchOptions.add(YIELD_SCORE_AS);
      searchOptions.add(alias);
    }
    return this;
  }

  /**
   * 设置向量相似度子句，同时会自动把向量参数写入 PARAMS。
   *
   * @param field 向量字段（通常形如 @vec）
   * @param paramName 参数占位符（可带或不带 '$'）
   * @param blob 查询向量的二进制内容
   */
  public FTHybridParams vsim(String field, String paramName, Object blob) {
    this.vectorField = field;
    this.vectorParam = normalizeParam(paramName);
    this.vectorBlob = blob;
    if (blob != null) {
      addParam(strip(paramName), blob);
    }
    return this;
  }

  public FTHybridParams vsimOptions(Object... options) {
    Collections.addAll(this.vsimOptions, options);
    return this;
  }

  /**
   * VSIM 子句中的预过滤表达式。
   * 等价于在 VSIM 块内部追加 FILTER "expr"。
   */
  public FTHybridParams vsimFilter(String filter) {
    if (filter != null) {
      this.vsimOptions.add(FILTER);
      this.vsimOptions.add(filter);
    }
    return this;
  }

  /**
   * 预过滤策略：ADHOC_BF
   */
  public FTHybridParams policyAdhocBF() {
    this.policyKeyword = ADHOC_BF;
    this.policyBatchSize = null;
    return this;
  }

  /**
   * 预过滤策略：BATCHES，并可选设置批大小。
   */
  public FTHybridParams policyBatches(Integer batchSize) {
    this.policyKeyword = BATCHES;
    this.policyBatchSize = batchSize;
    return this;
  }

  public FTHybridParams knn(int k, Integer efRuntime, String scoreAlias) {
    vsimOptions.clear();
    vsimOptions.add(KNN);
    int count = 0;
    // 占位 count
    vsimOptions.add(null);

    // K k
    vsimOptions.add(K);
    vsimOptions.add(k);
    count += 2;

    // EF_RUNTIME efRuntime
    if (efRuntime != null) {
      vsimOptions.add(EF_RUNTIME);
      vsimOptions.add(efRuntime);
      count += 2;
    }

    // YIELD_SCORE_AS alias（别名不计入 count）
    if (scoreAlias != null) {
      vsimOptions.add(YIELD_SCORE_AS);
      vsimOptions.add(scoreAlias);
    }

    // 回填 count
    vsimOptions.set(1, count);
    return this;
  }

  public FTHybridParams range(double radius, Double epsilon, String scoreAlias) {
    vsimOptions.clear();
    vsimOptions.add(RANGE);
    int count = 0;
    // 占位 count
    vsimOptions.add(null);

    vsimOptions.add(RADIUS);
    vsimOptions.add(radius);
    count += 2;

    if (epsilon != null) {
      vsimOptions.add(EPSILON);
      vsimOptions.add(epsilon);
      count += 2;
    }

    if (scoreAlias != null) {
      vsimOptions.add(YIELD_SCORE_AS);
      vsimOptions.add(scoreAlias);
    }

    vsimOptions.set(1, count);
    return this;
  }

  public FTHybridParams combineRrf(Integer window, Double constant, String alias) {
    combineOptions.clear();
    int count = 0;
    combineOptions.add(RRF);
    // 占位 count
    combineOptions.add(null);

    if (window != null) {
      combineOptions.add(WINDOW);
      combineOptions.add(window);
      count += 2;
    }
    if (constant != null) {
      combineOptions.add(CONSTANT);
      combineOptions.add(constant);
      count += 2;
    }
    if (alias != null) {
      combineOptions.add(YIELD_SCORE_AS);
      combineOptions.add(alias);
    }

    combineOptions.set(1, count);
    return this;
  }

  public FTHybridParams combineLinear(Double alpha, Double beta, Integer window, String alias) {
    combineOptions.clear();
    int count = 0;
    combineOptions.add(LINEAR);
    // 占位 count
    combineOptions.add(null);

    if (alpha != null) {
      combineOptions.add(ALPHA);
      combineOptions.add(alpha);
      count += 2;
    }
    if (beta != null) {
      combineOptions.add(BETA);
      combineOptions.add(beta);
      count += 2;
    }
    if (window != null) {
      combineOptions.add(WINDOW);
      combineOptions.add(window);
      count += 2;
    }
    if (alias != null) {
      combineOptions.add(YIELD_SCORE_AS);
      combineOptions.add(alias);
    }

    combineOptions.set(1, count);
    return this;
  }

  public FTHybridParams limit(int offset, int num) {
    this.limit = new int[]{offset, num};
    return this;
  }

  public FTHybridParams sortBy(String field, Object orderKeyword) {
    sortBy.add(field);
    if (orderKeyword != null) {
      sortBy.add(orderKeyword);
    }
    return this;
  }

  public FTHybridParams noSort() {
    this.noSort = true;
    return this;
  }

  public FTHybridParams loadAll() {
    this.loadAll = true;
    this.loadFields.clear();
    return this;
  }

  public FTHybridParams loadField(String field) {
    this.loadFields.add(field);
    return this;
  }

  public FTHybridParams loadField(String field, boolean decode) {
    loadField(field);
    addReturnFieldDecode(field, decode);
    return this;
  }

  public FTHybridParams groupBy(Object... args) {
    groupBy.clear();
    Collections.addAll(groupBy, args);
    return this;
  }

  public FTHybridParams apply(Object... args) {
    Collections.addAll(apply, args);
    return this;
  }

  public FTHybridParams filter(String filter) {
    if (filter != null) {
      filters.add(filter);
    }
    return this;
  }

  public FTHybridParams addParam(String name, Object value) {
    if (params == null) {
      params = new LinkedHashMap<>();
    }
    params.put(name, value);
    return this;
  }

  public FTHybridParams params(Map<String, Object> values) {
    if (values == null || values.isEmpty()) {
      return this;
    }
    if (params == null) {
      params = new LinkedHashMap<>(values);
    } else {
      params.putAll(values);
    }
    return this;
  }

  public FTHybridParams timeout(long timeoutMs) {
    this.timeout = timeoutMs;
    return this;
  }

  public FTHybridParams dialect(int dialect) {
    this.dialect = dialect;
    return this;
  }

  public FTHybridParams dialectOptional(int dialect) {
    if (dialect != 0 && this.dialect == null) {
      this.dialect = dialect;
    }
    return this;
  }

  public FTHybridParams extra(Object... args) {
    Collections.addAll(extraArgs, args);
    return this;
  }

  @Override
  public void addParams(CommandArguments args) {
    if (searchQuery == null || vectorField == null || vectorParam == null) {
      throw new IllegalStateException("SEARCH、VSIM 及其参数均为必填。");
    }
    args.add(SEARCH).add(searchQuery);
    if (!searchOptions.isEmpty()) {
      args.addObjects(searchOptions);
    }

    args.add(VSIM).add(vectorField).add(vectorParam);
    if (!vsimOptions.isEmpty()) {
      args.addObjects(vsimOptions);
    }
    // VSIM 预过滤策略（位于 VSIM 之后、COMBINE 之前）
    if (policyKeyword != null) {
      args.add(POLICY).add(policyKeyword);
      if (policyBatchSize != null) {
        args.add(BATCH_SIZE).add(policyBatchSize);
      }
    }

    if (!combineOptions.isEmpty()) {
      args.add(COMBINE).addObjects(combineOptions);
    }

    if (limit != null) {
      args.add(LIMIT).add(limit[0]).add(limit[1]);
    }

    if (!sortBy.isEmpty()) {
      args.add(SORTBY).add(sortBy.size()).addObjects(sortBy);
    }
    if (noSort) {
      args.add(NOSORT);
    }

    if (loadAll) {
      args.add(LOAD).add("*");
    } else if (!loadFields.isEmpty()) {
      args.add(LOAD).add(loadFields.size()).addObjects(loadFields);
    }

    if (!groupBy.isEmpty()) {
      args.add(GROUPBY).addObjects(groupBy);
    }
    if (!apply.isEmpty()) {
      args.add(APPLY).addObjects(apply);
    }
    if (!filters.isEmpty()) {
      filters.forEach(f -> args.add(FILTER).add(f));
    }

    if (params != null && !params.isEmpty()) {
      args.add(PARAMS).add(params.size() << 1);
      params.forEach((k, v) -> args.add(k).add(v));
    }

    if (timeout != null) {
      args.add(TIMEOUT).add(timeout);
    }
    if (dialect != null) {
      args.add(DIALECT).add(dialect);
    }

    if (!extraArgs.isEmpty()) {
      args.addObjects(extraArgs);
    }
  }

  public boolean hasContent() {
    return loadAll || !loadFields.isEmpty();
  }

  public Map<String, Boolean> getReturnFieldDecodeMap() {
    return returnFieldDecodeMap;
  }

  private void addReturnFieldDecode(String field, boolean decode) {
    if (returnFieldDecodeMap == null) {
      returnFieldDecodeMap = new HashMap<>();
    }
    returnFieldDecodeMap.put(field, decode);
  }

  private static String normalizeParam(String param) {
    if (param == null) {
      return null;
    }
    return param.startsWith("$") ? param : "$" + param;
  }

  private static String strip(String param) {
    return param != null && param.startsWith("$") ? param.substring(1) : param;
  }
}

