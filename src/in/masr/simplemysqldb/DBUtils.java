package in.masr.simplemysqldb;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;

public class DBUtils {

	@SuppressWarnings("rawtypes")
	public static void loadData(ResultSet set, Class c, Object o)
			throws Exception {
		Field[] fileds = c.getFields();
		for (Field field : fileds) {
			if (field.isAnnotationPresent(EntityColumn.class)) {
				String fieldName = field.getName();
				Class fieldType = field.getType();
				int lastDot = fieldType.getName().lastIndexOf('.');
				String fieldTypeName = fieldType.getName().substring(
						lastDot + 1);
				fieldTypeName = fieldTypeName.substring(0, 1).toUpperCase()
						+ fieldTypeName.substring(1);
				Method method = set.getClass().getMethod("get" + fieldTypeName,
						new Class[] { String.class });
				Object[] args = { new String(fieldName) };
				field.set(o, method.invoke(set, args));
			}
		}
	}

}
