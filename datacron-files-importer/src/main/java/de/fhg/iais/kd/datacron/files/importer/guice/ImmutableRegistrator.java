package de.fhg.iais.kd.datacron.files.importer.guice;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;

public class ImmutableRegistrator implements KryoRegistrator {

	public void registerClasses(Kryo kryo) {
		UnmodifiableCollectionsSerializer.registerSerializers(kryo);
		ImmutableListSerializer.registerSerializers(kryo);
		ImmutableSetSerializer.registerSerializers(kryo);
		ImmutableMapSerializer.registerSerializers(kryo);
		ImmutableMultimapSerializer.registerSerializers(kryo);		
	}

	

}
