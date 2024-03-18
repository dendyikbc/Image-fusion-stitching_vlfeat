package com.vlfeat.engine.model.desc;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Queue;
import java.util.TreeMap;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 *
 *
 * <pre>
 *   The execution order of each operator in the data model:
 *   join -> filter -> agg -> having -> sort -> limit.
 * </pre>
 */
public class DataModelDesc {

  public static class OutputField {
    private final boolean isDim;
    private final String alias;
    private final FunctionDesc function;
    private final String fullIdentifier;
    private final String relatedVertexId;
    private final ColumnDesc column;

    public OutputField(
        boolean isDim,
        String alias,
        FunctionDesc function,
        String fullIdentifier,
        String relatedVertexId,
        ColumnDesc column) {
      this.isDim = isDim;
      this.alias = alias;
      this.function = function;
      this.fullIdentifier = fullIdentifier;
      this.relatedVertexId = relatedVertexId;
      this.column = column;
    }

    public ColumnDesc getColumn() {
      return column;
    }

    public String getAlias() {
      return alias;
    }

    public FunctionDesc getFunction() {
      return function;
    }

    public String getFullIdentifier() {
      return fullIdentifier;
    }

    public String getRelatedVertexId() {
      return relatedVertexId;
    }

    public boolean isDim() {
      return isDim;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OutputField field = (OutputField) o;
      return java.util.Objects.equals(relatedVertexId, field.relatedVertexId)
          && java.util.Objects.equals(fullIdentifier, field.fullIdentifier);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(fullIdentifier, relatedVertexId);
    }
  }

  private String version = "1.0.0";

  private ModelEnums.TableType dataModelDescType = ModelEnums.TableType.BATCH;

  private String catalog;
  private String modelName = UUID.randomUUID().toString();
  private String factTable;
  private JoinDagDesc joinDag;
  private List<MeasureDesc> measures;
  private List<DimensionDesc> dimensions;
  @Nullable private FilterDesc filter;
  @Nullable private FilterDesc having;
  @Nullable private LimitDesc limit;
  @Nullable private SortDesc sort;
  @Nullable private PartitionDesc partitionDesc;
  @Nullable private WindowDesc windowDesc;
  @Nullable private UnionDagDesc unionDagDesc;
  @Nullable private StreamingDesc streamingDesc;
  @Nullable private ResourceDesc resourceDesc;
  @Nullable private OutputDesc outputDesc;
  private boolean defaultDtPartition;
  @Nullable private String comment;

  public DataModelDesc() {}

  public ModelEnums.TableType getDataModelDescType() {
    return dataModelDescType;
  }

  public void setDataModelDescType(ModelEnums.TableType dataModelDescType) {
    this.dataModelDescType = dataModelDescType;
  }

  @Nullable
  public UnionDagDesc getUnionDagDesc() {
    return unionDagDesc;
  }

  public void setUnionDagDesc(@Nullable UnionDagDesc unionDagDesc) {
    this.unionDagDesc = unionDagDesc;
  }

  @Nullable
  public StreamingDesc getStreamingDesc() {
    return streamingDesc;
  }

  public void setStreamingDesc(@Nullable StreamingDesc streamingDesc) {
    this.streamingDesc = streamingDesc;
  }

  @Nullable
  public ResourceDesc getResourceDesc() {
    return resourceDesc;
  }

  public void setResourceDesc(@Nullable ResourceDesc resourceDesc) {
    this.resourceDesc = resourceDesc;
  }

  @Nullable
  public OutputDesc getOutputDesc() {
    return outputDesc;
  }

  public void setOutputDesc(@Nullable OutputDesc outputDesc) {
    this.outputDesc = outputDesc;
  }

  public boolean isDefaultDtPartition() {
    return defaultDtPartition;
  }

  public void setDefaultDtPartition(boolean defaultDtPartition) {
    this.defaultDtPartition = defaultDtPartition;
  }

  @Nullable
  public String getComment() {
    return comment;
  }

  public void setComment(@Nullable String comment) {
    this.comment = comment;
  }

  @Nullable
  public PartitionDesc getPartitionDesc() {
    return partitionDesc;
  }

  public void setPartitionDesc(@Nullable PartitionDesc partitionDesc) {
    this.partitionDesc = partitionDesc;
  }

  public String getModelName() {
    return modelName;
  }

  public void setModelName(String modelName) {
    this.modelName = modelName;
  }

  public String getFactTable() {
    if (joinDag != null
        && joinDag.getVertices().stream()
            .anyMatch(v -> v.getVertexType().equals(ModelEnums.VertexType.MODEL))) {
      throw new UnsupportedOperationException("getFactTable is unsupported for model vertex");
    }
    return factTable;
  }

  public void setFactTable(String factTable) {
    if (joinDag != null
        && joinDag.getVertices().stream()
            .anyMatch(v -> v.getVertexType().equals(ModelEnums.VertexType.MODEL))) {
      throw new UnsupportedOperationException("setFactTable is unsupported for model vertex");
    }
    this.factTable = factTable;
  }

  public JoinDagDesc getJoinDag() {
    return joinDag;
  }

  public void setJoinDag(JoinDagDesc joinDag) {
    this.joinDag = joinDag;
  }

  public List<MeasureDesc> getMeasures() {
    return measures;
  }

  public void setMeasures(List<MeasureDesc> measures) {
    this.measures = measures;
  }

  public FilterDesc getFilter() {
    return filter;
  }

  public void setFilter(FilterDesc filter) {
    this.filter = filter;
  }

  public FilterDesc getHaving() {
    return having;
  }

  public void setHaving(FilterDesc having) {
    this.having = having;
  }

  public LimitDesc getLimit() {
    return limit;
  }

  public void setLimit(LimitDesc limit) {
    this.limit = limit;
  }

  public SortDesc getSort() {
    return sort;
  }

  public void setSort(SortDesc sort) {
    this.sort = sort;
  }

  @Nullable
  public WindowDesc getWindowDesc() {
    return windowDesc;
  }

  public void setWindowDesc(@Nullable WindowDesc windowDesc) {
    this.windowDesc = windowDesc;
  }

  public String getCatalog() {
    return catalog;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public String getVersion() {
    return this.version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public List<DimensionDesc> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<DimensionDesc> dimensions) {
    this.dimensions = dimensions;
  }

  /**
   * @return the order of the list returned by queryModel is, first dims, then measures.
   */
  public TreeMap<String, OutputField> extractOutputFields(boolean forMv) {
    TreeMap<String, OutputField> identifier2Field = new TreeMap<>();
    for (VertexDesc vertex : this.getJoinDag().getVertices()) {
      for (ColumnDesc column : vertex.getDimensionsColumns()) {
        OutputField field = column.toOutputField(vertex.getId(), forMv);
        identifier2Field.put(field.getFullIdentifier(), field);
      }
    }
    for (MeasureDesc measure : this.getMeasures()) {
      measure.check();
      String fullIdentifier = measure.getFunction().identifier();
      // todo(zhouyf) 待定 是否只用第一个vertexId
      String vertexId = measure.getFunction().getParams().get(0).getVertexId();
      OutputField field = measure.toOutputField(vertexId, forMv);
      identifier2Field.put(field.getFullIdentifier(), field);
    }
    return identifier2Field;
  }

  /** The number of dimensions in the model. */
  public int dimensionNum() {
    return (int)
        this.getJoinDag().getVertices().stream()
            .map(VertexDesc::getDimensionsColumns)
            .flatMap(List::stream)
            .count();
  }

  public void check() {
    Preconditions.checkArgument(
        !(getJoinDag() == null && getUnionDagDesc() == null),
        "Join dag & union dag cannot be null at the same time.");
    Preconditions.checkArgument(
        !(getJoinDag() != null && getUnionDagDesc() != null),
        "Join dag & union dag cannot have values at the same time.");
  }

  public DataModelDesc createWithNewMeasures(List<MeasureDesc> newMeasures) {
    DataModelDesc baseModel = new DataModelDesc();
    baseModel.setModelName(this.getModelName());
    baseModel.setFactTable(this.getFactTable());
    baseModel.setJoinDag(this.getJoinDag());
    baseModel.setMeasures(newMeasures);
    baseModel.setFilter(this.getFilter());
    baseModel.setHaving(this.getHaving());
    baseModel.setLimit(this.getLimit());
    baseModel.setSort(this.getSort());
    baseModel.setPartitionDesc(this.getPartitionDesc());
    return baseModel;
  }

  // Traverse to get a series of dataModelDesc, because the dataModelDesc may contain other vertexes
  // which are `DataModelDesc` too.
  public List<DataModelDesc> traverseDataModelDesc() {
    List<DataModelDesc> dataModelDescs = Lists.newArrayList();
    Queue<DataModelDesc> queue = Lists.newLinkedList();
    queue.add(this);
    while (!queue.isEmpty()) {
      DataModelDesc currentDataModelDesc = queue.poll();
      dataModelDescs.add(currentDataModelDesc);
      List<VertexDesc> vertices = currentDataModelDesc.getJoinDag().getVertices();
      for (VertexDesc vertex : vertices) {
        if (java.util.Objects.nonNull(vertex.getDataModel())) {
          queue.add(vertex.getDataModel());
        }
      }
    }
    return dataModelDescs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DataModelDesc)) {
      return false;
    }

    DataModelDesc that = (DataModelDesc) o;
    return Objects.equal(getVersion(), that.getVersion())
        && Objects.equal(getCatalog(), that.getCatalog())
        && Objects.equal(getModelName(), that.getModelName())
        && Objects.equal(getFactTable(), that.getFactTable())
        && Objects.equal(getJoinDag(), that.getJoinDag())
        && Objects.equal(getMeasures(), that.getMeasures())
        && Objects.equal(getFilter(), that.getFilter())
        && Objects.equal(getHaving(), that.getHaving())
        && Objects.equal(getLimit(), that.getLimit())
        && Objects.equal(getSort(), that.getSort())
        && Objects.equal(getPartitionDesc(), that.getPartitionDesc());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getVersion(),
        getCatalog(),
        getModelName(),
        getFactTable(),
        getJoinDag(),
        getMeasures(),
        getFilter(),
        getHaving(),
        getLimit(),
        getSort(),
        getPartitionDesc());
  }
}
