package com.box.dataplatform.test.util;

import java.io.*;

public class SerializableUtil {

  public static byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    ObjectOutputStream o = new ObjectOutputStream(b);
    o.writeObject(obj);
    return b.toByteArray();
  }

  public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    ByteArrayInputStream b = new ByteArrayInputStream(bytes);
    ObjectInputStream o = new ObjectInputStream(b);
    return o.readObject();
  }
}
