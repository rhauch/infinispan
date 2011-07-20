package org.infinispan.schematic;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.transaction.TransactionManager;

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.schematic.SchemaLibrary.Results;
import org.infinispan.schematic.SchematicEntry.FieldName;
import org.infinispan.schematic.document.Document;
import org.infinispan.schematic.document.EditableDocument;
import org.infinispan.schematic.document.Json;
import org.infinispan.schematic.internal.document.BasicDocument;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SchematicDbTest {

   private SchematicDb db;
   private EmbeddedCacheManager cm;

   private TransactionManager tm;

   @BeforeMethod
   public void beforeTest() {
      Configuration c = new Configuration();
      c = c.fluent().invocationBatching().build();
      cm = TestCacheManagerFactory.createCacheManager(c, true);
      // Now create the SchematicDb ...
      db = Schematic.get(cm, "documents");
      tm = TestingUtil.getTransactionManager(db.getCache());
   }

   @AfterMethod
   public void afterTest() {
      TestingUtil.killCacheManagers(cm);
      db = null;
      tm = null;
   }

   protected static InputStream resource(String resourcePath) {
      InputStream result = SchemaValidationTest.class.getClassLoader().getResourceAsStream(resourcePath);
      assert result != null : "Could not find resource \"" + resourcePath + "\"";
      return result;
   }

   protected void loadSchemas() throws IOException {
      SchemaLibrary schemas = db.getSchemaLibrary();
      schemas.put("http://json-schema.org/draft-03/schema#", Json.read(resource("json/schema/draft-03/schema.json")));
      schemas.put("json/schema/spec-example.json", Json.read(resource("json/schema/spec-example.json")));
   }

   @Test
   public void shouldStoreDocumentWithUnusedKeyAndWithNullMetadata() {
      Document doc = Schematic.newDocument("k1", "value1", "k2", 2);
      String key = "can be anything";
      SchematicEntry prior = db.put(key, doc, null);
      assert prior == null : "Should not have found a prior entry";
      SchematicEntry entry = db.get(key);
      assert entry != null : "Should have found the entry";

      // Verify the content ...
      Document read = entry.getContentAsDocument();
      assert read != null;
      assert "value1".equals(read.getString("k1"));
      assert 2 == read.getInteger("k2");
      assert entry.getContentAsBinary() == null : "Should not have a Binary value for the entry's content";
      assert read.containsAll(doc);
      assert read.equals(doc);

      // Verify the metadata ...
      Document readMetadata = entry.getMetadata();
      assert readMetadata != null;
      assert readMetadata.getString("id").equals(key);
   }

   @Test
   public void shouldStoreDocumentWithUnusedKeyAndWithNonNullMetadata() {
      Document doc = Schematic.newDocument("k1", "value1", "k2", 2);
      Document metadata = Schematic.newDocument("mimeType", "text/plain");
      String key = "can be anything";
      SchematicEntry prior = db.put(key, doc, metadata);
      assert prior == null : "Should not have found a prior entry";

      // Read back from the database ...
      SchematicEntry entry = db.get(key);
      assert entry != null : "Should have found the entry";

      // Verify the content ...
      Document read = entry.getContentAsDocument();
      assert read != null;
      assert "value1".equals(read.getString("k1"));
      assert 2 == read.getInteger("k2");
      assert entry.getContentAsBinary() == null : "Should not have a Binary value for the entry's content";
      assert read.containsAll(doc);
      assert read.equals(doc);

      // Verify the metadata ...
      Document readMetadata = entry.getMetadata();
      assert readMetadata != null;
      assert readMetadata.getString("mimeType").equals(metadata.getString("mimeType"));
      assert readMetadata.containsAll(metadata);
      // metadata contains more than what we specified ...
      assert !readMetadata.equals(metadata) : "Expected:\n" + metadata + "\nFound: \n" + readMetadata;
   }

   @Test
   public void shouldStoreDocumentAndValidateAfterRefetching() throws Exception {
      loadSchemas();
      Document doc = Json.read(resource("json/spec-example-doc.json"));
      String key = "json/spec-example-doc.json";
      String schemaUri = "json/schema/spec-example.json";
      Document metadata = new BasicDocument(FieldName.SCHEMA_URI, schemaUri);
      db.put(key, doc, metadata);
      Results results = db.getSchemaLibrary().validate(doc, schemaUri);
      assert !results.hasProblems() : "There are validation problems: " + results;

      SchematicEntry actualEntry = db.get(key);
      Document actualMetadata = actualEntry.getMetadata();
      Document actualDocument = actualEntry.getContentAsDocument();
      assert actualMetadata != null;
      assert actualDocument != null;
      assert schemaUri.equals(actualMetadata.getString(FieldName.SCHEMA_URI)) : "The $schema in the metadata doesn't match: "
               + metadata;
      assert key.equals(actualMetadata.getString(FieldName.ID)) : "The id in the metadata doesn't match: " + metadata;

      // Validate just the document ...
      results = db.validate(key);
      assert !results.hasProblems() : "There are validation problems: " + results;

      // Now validate the whole database ...
      Map<String, Results> resultsByKey = db.validate(key, "non-existant");
      assert resultsByKey != null;
      assert !resultsByKey.containsKey(key) : "There are validation problems: " + resultsByKey.get(key);

      // Now validate the whole database ...
      resultsByKey = db.validateAll();
      assert resultsByKey != null;
      assert !resultsByKey.containsKey(key) : "There are validation problems: " + resultsByKey.get(key);
   }

   @Test
   public void shouldStoreDocumentAndFetchAndModifyAndRefetch() throws Exception {
      // Store the document ...
      Document doc = Schematic.newDocument("k1", "value1", "k2", 2);
      Document metadata = Schematic.newDocument("mimeType", "text/plain");
      String key = "can be anything";
      SchematicEntry prior = db.put(key, doc, metadata);
      assert prior == null : "Should not have found a prior entry";

      // Read back from the database ...
      SchematicEntry entry = db.get(key);
      assert entry != null : "Should have found the entry";

      // Verify the content ...
      Document read = entry.getContentAsDocument();
      assert read != null;
      assert "value1".equals(read.getString("k1"));
      assert 2 == read.getInteger("k2");
      assert entry.getContentAsBinary() == null : "Should not have a Binary value for the entry's content";
      assert read.containsAll(doc);
      assert read.equals(doc);

      // Modify using an editor ...
      try {
         // tm.begin();
         EditableDocument editable = entry.editDocumentContent();
         editable.setBoolean("k3", true);
         editable.setNumber("k4", 3.5d);
      } finally {
         // tm.commit();
      }

      // Now re-read ...
      SchematicEntry entry2 = db.get(key);
      Document read2 = entry2.getContentAsDocument();
      assert read2 != null;
      assert "value1".equals(read2.getString("k1"));
      assert 2 == read2.getInteger("k2");
      assert true == read2.getBoolean("k3");
      assert 3.4d < read2.getDouble("k4");
   }
}
