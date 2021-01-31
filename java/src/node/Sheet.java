package node;

import java.util.List;

public class Sheet {
	//sheet名称
	private String name;
	//sheet标题行
	private List<String> title;
	//查询sql
	private String sql;
	//序号
	private String serial;
	//标题行高
	private int titleHeight;

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<String> getTitle() {
		return title;
	}
	public void setTitle(List<String> title) {
		this.title = title;
	}
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public String getSerial() {
		return serial;
	}
	public void setSerial(String serial) {
		this.serial = serial;
	}

	public void setTitleHeight(int titleHeight) {
		this.titleHeight = titleHeight;
	}

	public int getTitleHeight() {
		return titleHeight;
	}
}
