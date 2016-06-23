

package com.flink.cep;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.Objects;

//import java.util.Objects;

public class Event_d {
	private String id;
	private String name;
	private String price;
	private String md5;

	public Event_d(String id, String name, String price, String md5) {
		this.id = id;
		this.name = name;
		this.price = price;
		this.md5 = md5;
	}

	public String getMd5() {
		return md5;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setMd5(String md5) {
		this.md5 = md5;
	}

	public String getPrice() {
		return price;
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return super.toString();
	}

	public boolean equals(Object obj) {
		if (obj instanceof Event_d) {
			Event_d other = (Event_d) obj;

			return name.equals(other.name) && price == other.price && id == other.id;
		} else {
			return false;
		}
	}

	public int hashCode() {
		return Objects.hash(name, price, id);
	}

	public static TypeSerializer<Event_d> createTypeSerializer() {
		TypeInformation<Event_d> typeInformation = (TypeInformation<Event_d>) TypeExtractor.createTypeInfo(Event_d.class);

		return typeInformation.createSerializer(new ExecutionConfig());
	}
}
