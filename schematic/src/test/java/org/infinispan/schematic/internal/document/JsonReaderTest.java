package org.infinispan.schematic.internal.document;

import org.infinispan.schematic.document.Document;
import org.infinispan.schematic.document.Null;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "bson.impl.JsonReaderTest")
public class JsonReaderTest {

   protected JsonReader reader;
   protected boolean print;
   protected Document doc;

   @BeforeMethod
   public void beforeTest() {
      reader = new JsonReader();
      print = false;
   }

   @AfterMethod
   public void afterTest() {
      reader = null;
   }

   @Test
   public void shouldParseEmptyDocument() throws Exception {
      doc = reader.read("{ }");
   }

   @Test
   public void shouldParseEmptyDocumentWithLeadingWhitespace() throws Exception {
      doc = reader.read("  { }");
   }

   @Test
   public void shouldParseEmptyDocumentWithEmbeddedWhitespace() throws Exception {
      doc = reader.read("{ \n \t \r\n }  ");
   }

   @Test
   public void shouldParseDocumentWithSingleFieldWithStringValue() throws Exception {
      doc = reader.read("{ \"foo\" : \"bar\" }");
      assertField("foo", "bar");
   }

   @Test
   public void shouldParseDocumentWithSingleFieldWithBooleanFalseValue() throws Exception {
      doc = reader.read("{ \"foo\" : false }");
      assertField("foo", false);
   }

   @Test
   public void shouldParseDocumentWithSingleFieldWithBooleanTrueValue() throws Exception {
      doc = reader.read("{ \"foo\" : true }");
      assertField("foo", true);
   }

   @Test
   public void shouldParseDocumentWithSingleFieldWithIntegerValue() throws Exception {
      doc = reader.read("{ \"foo\" : 0 }");
      assertField("foo", (int) 0);
   }

   @Test
   public void shouldParseDocumentWithSingleFieldWithLongValue() throws Exception {
      long val = (long) Integer.MAX_VALUE + 10L;
      doc = reader.read("{ \"foo\" : " + val + " }");
      assertField("foo", val);
   }

   @Test
   public void shouldParseDocumentWithSingleFieldWithDoubleValue() throws Exception {
      doc = reader.read("{ \"foo\" : 2.3543 }");
      assertField("foo", 2.3543d);
   }

   @Test
   public void shouldParseDocumentWithMultipleFieldsAndExtraDelimiters() throws Exception {
      doc = reader.read("{ \"foo\" : 32 ,, \"nested\" : { \"bar\" : \"baz\", \"bom\" : true } ,}");
      assertField("foo", 32);
      doc = doc.getDocument("nested");
      assertField("bar", "baz");
      assertField("bom", true);
   }

   protected void assertField(String name, Object value) {
      Object actual = doc.get(name);
      if (value == null) {
         assert Null.matches(actual);
      }
      assert value.equals(actual);
   }

}
