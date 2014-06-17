package com.twitter.elephantbird.hive.serde;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public final class ProtobufStructObjectInspector extends SettableStructObjectInspector implements Externalizable{

  private static void writeProto(ObjectOutput out, DescriptorProtos.FileDescriptorProto proto) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(131072);
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(proto);
        oos.close();
        byte[] serializedProto = baos.toByteArray();
        out.write(serializedProto);  
    }
    
    private static DescriptorProtos.FileDescriptorProto readProto(ObjectInput in) throws IOException, ClassNotFoundException {
        byte[] serializedProto = new byte[131072]; 
        in.read(serializedProto);
        ByteArrayInputStream bais = new ByteArrayInputStream(serializedProto);
        ObjectInputStream ois = new ObjectInputStream(bais);
        DescriptorProtos.FileDescriptorProto proto = (DescriptorProtos.FileDescriptorProto)ois.readObject();
        ois.close();
        return proto;
    }
    
  public static class ProtobufStructField implements StructField, Externalizable {

    private ObjectInspector oi = null;
    private String comment = null;
    private FieldDescriptor fieldDescriptor;

    public ProtobufStructField() { super(); }
    
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        Descriptor containingMessageType = fieldDescriptor.getContainingType();
        DescriptorProtos.FileDescriptorProto proto = containingMessageType.getFile().toProto();
        int messageIndex = containingMessageType.getIndex();
        int fieldIndex = fieldDescriptor.getIndex();
        out.writeInt(fieldIndex);
        out.writeInt(messageIndex);
        writeProto(out, proto); 
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int fieldIdx = in.readInt();
        int messageIdx = in.readInt();
        DescriptorProtos.FileDescriptorProto proto = readProto(in);
        FileDescriptor fd = null;
        try {
            fd = FileDescriptor.buildFrom(proto, new FileDescriptor[]{});
            
        } catch (Descriptors.DescriptorValidationException ex) {
            throw new IOException(ex);
        }
        Descriptor message = fd.getMessageTypes().get(messageIdx);
        fieldDescriptor = message.getFields().get(fieldIdx);
        oi = this.createOIForField();
    }
    
    
    @SuppressWarnings("unchecked")
    public ProtobufStructField(FieldDescriptor fieldDescriptor) {
      this.fieldDescriptor = fieldDescriptor;
      oi = this.createOIForField();
    }

    @Override
    public String getFieldName() {
      return fieldDescriptor.getName();
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return oi;
    }

    @Override
    public String getFieldComment() {
      return comment;
    }

    public FieldDescriptor getFieldDescriptor() {
      return fieldDescriptor;
    }

    private PrimitiveCategory getPrimitiveCategory(JavaType fieldType) {
      switch (fieldType) {
        case INT:
          return PrimitiveCategory.INT;
        case LONG:
          return PrimitiveCategory.LONG;
        case FLOAT:
          return PrimitiveCategory.FLOAT;
        case DOUBLE:
          return PrimitiveCategory.DOUBLE;
        case BOOLEAN:
          return PrimitiveCategory.BOOLEAN;
        case STRING:
          return PrimitiveCategory.STRING;
        case BYTE_STRING:
          return PrimitiveCategory.BINARY;
        case ENUM:
          return PrimitiveCategory.STRING;
        default:
          return null;
      }
    }

    private ObjectInspector createOIForField() {
      JavaType fieldType = fieldDescriptor.getJavaType();
      PrimitiveCategory category = getPrimitiveCategory(fieldType);
      ObjectInspector elementOI = null;
      if (category != null) {
        elementOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(category);
      } else {
        switch (fieldType) {
          case MESSAGE:
            elementOI = new ProtobufStructObjectInspector(fieldDescriptor.getMessageType());
            break;
          default:
            throw new RuntimeException("JavaType " + fieldType
                + " from protobuf is not supported.");
        }
      }
      if (fieldDescriptor.isRepeated()) {
        return ObjectInspectorFactory.getStandardListObjectInspector(elementOI);
      } else {
        return elementOI;
      }
    }
  }

  private Descriptor descriptor;
  private List<StructField> structFields = Lists.newArrayList();

  
  public ProtobufStructObjectInspector() { super(); }
  
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        DescriptorProtos.FileDescriptorProto proto = descriptor.getFile().toProto();
        out.writeInt(descriptor.getIndex());
        writeProto(out,proto);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int idx = in.readInt();
        DescriptorProtos.FileDescriptorProto proto = readProto(in);
        FileDescriptor fd = null;
        try {
            fd = FileDescriptor.buildFrom(proto, new FileDescriptor[]{});
            
        } catch (Descriptors.DescriptorValidationException ex) {
            throw new IOException(ex);
        }
        descriptor = fd.getMessageTypes().get(idx);
        populateStructFields();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 31 * hash + (this.descriptor != null ? this.descriptor.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ProtobufStructObjectInspector other = (ProtobufStructObjectInspector) obj;
        if (this.descriptor != other.descriptor && (this.descriptor == null || !this.descriptor.equals(other.descriptor))) {
            return false;
        }
        return true;
    }
    
    
  
  ProtobufStructObjectInspector(Descriptor descriptor) {
    this.descriptor = descriptor;
    populateStructFields();
  }

  private void populateStructFields() {
      for (FieldDescriptor fd : descriptor.getFields()) {
      structFields.add(new ProtobufStructField(fd));
    }
  }
  
  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public String getTypeName() {
    StringBuilder sb = new StringBuilder("struct<");
    boolean first = true;
    for (StructField structField : getAllStructFieldRefs()) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(structField.getFieldName()).append(":")
          .append(structField.getFieldObjectInspector().getTypeName());
    }
    sb.append(">");
    return sb.toString();
  }

  @Override
  public Object create() {
    return descriptor.toProto().toBuilder().build();
  }

  @Override
  public Object setStructFieldData(Object data, StructField field, Object fieldValue) {
    return ((Message) data)
        .toBuilder()
        .setField(descriptor.findFieldByName(field.getFieldName()), fieldValue)
        .build();
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return structFields;
  }

  @Override
  public Object getStructFieldData(Object data, StructField structField) {
    if (data == null) {
      return null;
    }
    Message m = (Message) data;
    ProtobufStructField psf = (ProtobufStructField) structField;
    FieldDescriptor fieldDescriptor = psf.getFieldDescriptor();
    Object result = m.getField(fieldDescriptor);
    if (fieldDescriptor.getType() == Type.ENUM) {
      return ((EnumValueDescriptor)result).getName();
    }
    if (fieldDescriptor.getType() == Type.BYTES && (result instanceof ByteString)) {
        return ((ByteString)result).toByteArray();
    }
    return result;
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    return new ProtobufStructField(descriptor.findFieldByName(fieldName));
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    List<Object> result = Lists.newArrayList();
    Message m = (Message) data;
    for (FieldDescriptor fd : descriptor.getFields()) {
      result.add(m.getField(fd));
    }
    return result;
  }
}
