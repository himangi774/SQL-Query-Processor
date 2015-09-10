#include <bits/stdc++.h>

using namespace std;

//structure definations
typedef map < string,  map < string, vector < int > > >  dbtype ;  // map< tablename,  map< column_name, vector<values> > > 
typedef map < string,  map < int , string > > dbreftype ;  // map <tablename , map<order, column_name>>
struct total_db{
	dbtype data; //original database
	dbreftype ref; //column reference to know the order
};
struct parsed_query{
	vector< string > columns;
	vector< string > tables;
	vector< string > comparisions;
	vector< string > operators;
	vector< string > agg;
};

struct mined_data{
	int total_tables; // number of tables
	dbtype mydb; //database
	map < string , int > col_to_index; //tablename.columnname to index
	vector < string > table_order;  //table order
	vector < int > table_sizes; // all tables size
	vector < vector < string > > columns; // vector of columns in all tables
	vector < int > row_pointer; // my b in python row_pointer
	vector < int > candidate; // candidate
};

//global variables
bool error=false;
vector< vector < int > > join_table;
set< string > to_remove;
double g_avg=0;
long long int g_sum=0;
long long int g_min=INT_MAX;
long long int g_max=INT_MIN;

//function prototypes
bool is_number(string input);
struct parsed_query * parseit(string input);
struct total_db * read_metadata();
struct total_db * populate_db(struct total_db * mydb);
struct parsed_query * verify_wrapper(struct total_db * mydb,struct parsed_query * input_query,string cur_column,int index_string,bool flag);
struct parsed_query * verify_extend_query(struct total_db * mydb,struct parsed_query * input_query);
void print_db(struct total_db * mydb);
void print_query(struct parsed_query * query_parsed);
struct mined_data * populate_mine(struct parsed_query * query_parsed, struct total_db * mydb);
struct mined_data * carry_over(struct mined_data * mymine, struct total_db * mydb,struct parsed_query * query_parsed);
void create_candidate(struct mined_data * mymine, struct total_db * mydb,struct parsed_query * query_parsed);
void miner(struct mined_data * mymine, struct total_db * mydb,struct parsed_query * query_parsed);
bool accept_on_where(struct mined_data * mymine,struct parsed_query * query_parsed);
bool check_where(struct mined_data *mymine,string op,struct parsed_query * query_parsed);
void select_from_db(struct mined_data * mymine,struct parsed_query * query_parsed);
bool agg_func_handler(string agg_func,struct total_db * mydb,struct parsed_query * query_parsed);
void parse_distinct(struct mined_data* mymine,struct parsed_query * query_parsed);

int main(int argc,char * argv[])
{
	if(argc!=2)
	{
		cout<<"Please Provide input query\n";
		return 0;
	}
	join_table.clear();
	to_remove.clear();
	struct total_db *  universal_db=read_metadata(); //final data stored here
	universal_db=populate_db(universal_db);
	struct parsed_query * query_parsed=parseit(argv[1]);
	query_parsed=verify_extend_query(universal_db,query_parsed);
	//print_query(query_parsed);
	//	print_db(universal_db);
	if(error)
		cout<<"error"<<endl;
	else
	{
		struct mined_data * mymine = populate_mine(query_parsed,universal_db);
		string agg_func=query_parsed->agg[0];
		//check for agg functions
		if(agg_func_handler(query_parsed->agg[0],universal_db,query_parsed))
			return 0;
		miner(mymine,universal_db,query_parsed);
		parse_distinct(mymine,query_parsed);
		select_from_db(mymine,query_parsed);
	}
	return 0;
}

void parse_distinct(struct mined_data* mymine,struct parsed_query * query_parsed)
{
	int i;
	bool flag=false;
	for(i=0;i<query_parsed->agg.size();i++)
	{
		if(query_parsed->agg[i]=="distinct")
		{
			flag=true;
			break;
		}
	}
	if(!flag)
		return;
	string col_name=query_parsed->columns[i];
	set< int > dis_col;
	int column_index=mymine->col_to_index[col_name];
	for(int i=0;i<join_table.size();i++)
	{
		if(dis_col.find(join_table[i][column_index])==dis_col.end())
			dis_col.insert(join_table[i][column_index]);
		else
		{
			join_table.erase(join_table.begin()+i);
			i--;
		}
	}
	return;
}

bool agg_func_handler(string agg_func,struct total_db * mydb,struct parsed_query * query_parsed)
{
	if(agg_func=="max" or agg_func=="min" or agg_func=="avg" or agg_func=="sum")
	{
		g_avg=0;
		string c_name=query_parsed->columns[0];
		size_t found_pos=c_name.find("."); 
		c_name=c_name.substr(found_pos+1);
		string t_name=query_parsed->tables[0];
		vector< int > rel_data = mydb->data[t_name][c_name];
		for(int i=0;i<rel_data.size();i++)
		{
			g_max=g_max>=rel_data[i]?g_max:rel_data[i];
			g_min=g_min<=rel_data[i]?g_min:rel_data[i];
			g_avg+=rel_data[i];
		}
		g_sum=g_avg;
		g_avg=g_avg/(1.0*rel_data.size());
		cout<<agg_func<<"("<<query_parsed->columns[0]<<")"<<endl;
		if(agg_func=="max")
			cout<<g_max<<endl;
		else if(agg_func=="min")
			cout<<g_min<<endl;
		else if(agg_func=="avg")
		{
			cout.setf(ios::fixed);
			cout<<setprecision(4)<<g_avg<<endl;
		}
		else if(agg_func=="sum")
			cout<<g_sum<<endl;
		return true;
	}
	return false;

}
void select_from_db(struct mined_data * mymine,struct parsed_query * query_parsed)
{
	int i,j;
	bool flag=0;
	for(i=0;i<query_parsed->columns.size();i++)
	{
		if(i!=0 and flag==1)
		{
			flag=0;
			cout<<",";
		}
		if(to_remove.find(query_parsed->columns[i])==to_remove.end())
		{
			cout<<query_parsed->columns[i];
			flag=1;
		}
	}
	cout<<endl;
	for(i=0;i<join_table.size();i++)
	{
		for(j=0;j<query_parsed->columns.size();j++)
		{
			if(j!=0 and flag==1)
			{
				flag=0;
				cout<<",";
			}
			if(to_remove.find(query_parsed->columns[j])==to_remove.end())
			{
				cout<<join_table[i][mymine->col_to_index[query_parsed->columns[j]]];
				flag=1;
			}
		}
		cout<<endl;
	}
}
bool check_where(struct mined_data *mymine,string op,struct parsed_query * query_parsed)
{
	vector< int > cand(mymine->candidate);
	string token;
	string comp;
	stringstream cond;
	cond<<op;
	vector< string > check; // A  <= B
	while(cond>>token)
	{
		check.push_back(token);
	}
	comp=check[1];
	if(comp=="=")
	{
		if(is_number(check[0]))
		{
			if(atoi(check[0].c_str())==cand[mymine->col_to_index[check[2]]])
				return true;
		}
		else if(is_number(check[2]))
		{
			if(cand[mymine->col_to_index[check[0]]]==atoi(check[2].c_str()))
				return true;
		}
		else if(cand[mymine->col_to_index[check[0]]]==cand[mymine->col_to_index[check[2]]])
		{
			//removing duplicate column
			to_remove.insert(check[2]);
			return true;
		}
	}
	else if(comp==">")
	{
		if(is_number(check[0]))
		{
			if(atoi(check[0].c_str())>cand[mymine->col_to_index[check[2]]])
				return true;
		}
		else if(is_number(check[2]))
		{
			if(cand[mymine->col_to_index[check[0]]]>atoi(check[2].c_str()))
				return true;
		}
		else if(cand[mymine->col_to_index[check[0]]]>cand[mymine->col_to_index[check[2]]])
			return true;
	}
	else if(comp=="<")
	{
		if(is_number(check[0]))
		{
			if(atoi(check[0].c_str())<cand[mymine->col_to_index[check[2]]])
				return true;
		}
		else if(is_number(check[2]))
		{
			if(cand[mymine->col_to_index[check[0]]]<atoi(check[2].c_str()))
				return true;
		}
		else if(cand[mymine->col_to_index[check[0]]]<cand[mymine->col_to_index[check[2]]])
			return true;
	}
	else if(comp=="<=")
	{
		if(is_number(check[0]))
		{
			if(atoi(check[0].c_str())<=cand[mymine->col_to_index[check[2]]])
				return true;
		}
		else if(is_number(check[2]))
		{
			if(cand[mymine->col_to_index[check[0]]]<=atoi(check[2].c_str()))
				return true;
		}
		else if(cand[mymine->col_to_index[check[0]]]<=cand[mymine->col_to_index[check[2]]])
			return true;
	}
	else if(comp==">=")
	{
		if(is_number(check[0]))
		{
			if(atoi(check[0].c_str())>=cand[mymine->col_to_index[check[2]]])
				return true;
		}
		else if(is_number(check[2]))
		{
			if(cand[mymine->col_to_index[check[0]]]>=atoi(check[2].c_str()))
				return true;
		}
		else if(cand[mymine->col_to_index[check[0]]]>=cand[mymine->col_to_index[check[2]]])
			return true;
	}
	else if(comp=="!=")
	{
		if(is_number(check[0]))
		{
			if(atoi(check[0].c_str())!=cand[mymine->col_to_index[check[2]]])
				return true;
		}
		else if(is_number(check[2]))
		{
			if(cand[mymine->col_to_index[check[0]]]!=atoi(check[2].c_str()))
				return true;
		}
		else if(cand[mymine->col_to_index[check[0]]]!=cand[mymine->col_to_index[check[2]]])
			return true;
	}
	return false;
}
bool accept_on_where(struct mined_data * mymine,struct parsed_query * query_parsed)
{
	if(query_parsed->comparisions.size()==0)
		return true;
	bool accept=true;
	vector<bool> accepts;
	for(int i=0;i<query_parsed->comparisions.size();i++)
	{
		accepts.push_back(check_where(mymine,query_parsed->comparisions[i],query_parsed));
	}
	accept=accepts[0];
	if(query_parsed->operators.size()==0)
		return accept;
	for(int i=0;i<query_parsed->operators.size();i++)
	{
		if(query_parsed->operators[i]=="and")
			accept=accept and accepts[i+1];
		else if(query_parsed->operators[i]=="or")
			accept=accept or accepts[i+1];
	}
	return accept;
}

void create_candidate(struct mined_data * mymine, struct total_db * mydb,struct parsed_query * query_parsed)
{
	vector < int > row_p=mymine->row_pointer;
	string table_name,t_col;
	for(int i=0;i<row_p.size();i++)
	{
		table_name=mymine->table_order[i];
		for(int j=0;j<mymine->columns[i].size();j++)
		{
			t_col=table_name+"."+mymine->columns[i][j];
			mymine->candidate[mymine->col_to_index[t_col]]=mydb->data[table_name][mymine->columns[i][j]][mymine->row_pointer[i]];
		}
	}
	if(accept_on_where(mymine,query_parsed))
	{
		join_table.push_back(mymine->candidate);
	}
}

struct mined_data * carry_over(struct mined_data * mymine, struct total_db * mydb,struct parsed_query * query_parsed)
{
	int first=0;
	int back=(mymine->total_tables)-1;
	while(mymine->row_pointer[first]>=mymine->table_sizes[first])
	{
		if(first==back)
			break;
		mymine->row_pointer[first+1]+=1;
		mymine->row_pointer[first]=0;
		first++;
	}
	if(mymine->row_pointer[back]<mymine->table_sizes[back])
		create_candidate(mymine,mydb,query_parsed);
	return mymine;
}

void miner(struct mined_data * mymine, struct total_db * mydb,struct parsed_query * query_parsed)
{
	int back=(mymine->total_tables)-1;
	while(1)
	{
		mymine=carry_over(mymine,mydb,query_parsed);
		mymine->row_pointer[0]+=1;
		if(mymine->row_pointer[back]>=mymine->table_sizes[back])
			break;
	}
}

struct mined_data * populate_mine(struct parsed_query * query_parsed, struct total_db * mydb)
{
	struct mined_data * mymine=new struct mined_data;
	vector < string > tab_cols;
	string cur_table;
	int col_count=0;
	for(int i=0;i<query_parsed->tables.size();i++)
	{
		cur_table=query_parsed->tables[i];
		tab_cols.clear();
		mymine->table_order.push_back(cur_table);
		map < string , vector < int > >:: iterator it;
		for(it=mydb->data[cur_table].begin();it!=mydb->data[cur_table].end();it++)
		{
			tab_cols.push_back(it->first);
			mymine->col_to_index[cur_table+"."+it->first]=col_count;
			col_count++;
		}
		mymine->columns.push_back(tab_cols);
		it--;
		mymine->table_sizes.push_back(mydb->data[cur_table][it->first].size());
	}
	mymine->total_tables=mymine->table_sizes.size();
	for(int i=0;i<col_count;i++)
		mymine->candidate.push_back(0);
	for(int i=0;i<mymine->total_tables;i++)
		mymine->row_pointer.push_back(0);
	return mymine;
}

bool is_number(string input)
{
	if(input[0]=='+' or input[0]=='-' or isdigit(input[0]))
	{
		for(int i=1;i<input.size();i++)
		{
			if(!isdigit(input[i]))
				return false;
		}
	}
	else return false;
	return true;

}

struct parsed_query * verify_wrapper(struct total_db * mydb,struct parsed_query * input_query,string cur_column,int index_string,bool flag)
{
	size_t found_pos;
	found_pos=cur_column.find("("); //check for agg functions
	if(found_pos!=string::npos) 
	{   
		input_query->agg.push_back(cur_column.substr(0,found_pos));
		cur_column=cur_column.substr(found_pos+1);
		found_pos=cur_column.find(")");
		cur_column=cur_column.substr(0,found_pos);
	}   
	else
	{   
		input_query->agg.push_back("");
	}  

	found_pos=cur_column.find(".");
	if(found_pos!=string::npos) // if table1.c types check if c in table1
	{
		if(mydb->data.find(cur_column.substr(0,found_pos))==mydb->data.end())
			error=true;
		else if(mydb->data[cur_column.substr(0,found_pos)].find(cur_column.substr(found_pos+1))==mydb->data[cur_column.substr(0,found_pos)].end())
			error=true;

	}
	else//if B, then does b exist in a table and if more than one error and replace B with tablex.B
	{
		int count=0;
		string newcolname="",token="",mycomp="";
		for(int i=0;i<(input_query->tables).size();i++)
		{
			if(mydb->data[input_query->tables[i]].find(cur_column)!=mydb->data[input_query->tables[i]].end())
			{
				count++;
				if(flag)
				{
					newcolname="";
					newcolname+=input_query->tables[i]+"."+cur_column;
					input_query->columns[index_string]=newcolname;
				}
				else if(flag==0)
				{
					newcolname="";
					token="";
					stringstream comp;
					newcolname+=input_query->tables[i]+"."+cur_column;
					mycomp=input_query->comparisions[index_string];
					input_query->comparisions[index_string]="";
					comp<<mycomp;
					while(comp>>token)
					{
						if(token.compare(cur_column)==0)
						{
							input_query->comparisions[index_string]+=newcolname+" ";
						}
						else
						{
							input_query->comparisions[index_string]+=token+" ";
						}
					}
					input_query->comparisions[index_string].pop_back();

				}
			}
		}
		if(count!=1)
		{
			error=true;
			return input_query;
		}
	}
	return input_query;

}
struct parsed_query * verify_extend_query(struct total_db * mydb,struct parsed_query * input_query)
{
	//to handle *
	if(input_query->columns.size()==1 and input_query->columns[0].compare("*")==0)
	{
		input_query->columns.clear();
		string newcolumns="";
		map< string, vector< int > >::iterator colit;
		for(int i=0;i<((input_query->tables).size());i++)
		{
			for(colit=mydb->data[input_query->tables[i]].begin();colit!=mydb->data[input_query->tables[i]].end();colit++)
			{
				newcolumns="";
				newcolumns+=input_query->tables[i]+"."+colit->first;
				input_query->columns.push_back(newcolumns);
			}
		}
	}
	//input query tables exist in db or not
	for(int i=0;i<((input_query->tables).size());i++)
	{
		if(mydb->data.find(input_query->tables[i])==mydb->data.end())
		{	
			error=true;
			return input_query;
		}
	}
	string cur_column;
	//for input column check
	for(int i=0;i<(input_query->columns).size();i++)
	{
		cur_column=input_query->columns[i];
		input_query=verify_wrapper(mydb,input_query,cur_column,i,true);
		if(error==true)
			return input_query;

	}
	//where check
	string token="";
	int count_t=0;
	for(int i=0;i<input_query->comparisions.size();i++)
	{
		cur_column=input_query->comparisions[i];
		stringstream comp;
		token="";
		comp<<cur_column;
		count_t=0;
		while(comp>>token)
		{
			if((count_t==0) or (count_t==2))
			{
				// Check if token is not number
				if(!is_number(token))
				{
					input_query=verify_wrapper(mydb,input_query,token,i,false);
					if(error==true)
						return input_query;
				}
			}
			token="";
			count_t++;
		}
	}
	return input_query;
}

void print_db(struct total_db * mydb)
{
	dbtype::iterator it;
	for(it = (mydb->data).begin(); it!=(mydb->data).end();it++)
	{
		cout<<"Tablename: "<<it->first<<endl;
		map < string, vector < int > >::iterator ct;
		for(ct=mydb->data[it->first].begin();ct!=mydb->data[it->first].end();ct++)
		{
			cout<<"Column Name: "<<ct->first<<endl;
			cout<<" .... Column Data ... "<<endl;
			for(int i=0;i<(ct->second).size();i++)
			{
				cout<<ct->second[i]<<endl;
			}
		}

	}
}


void print_query(struct parsed_query * query_parsed)
{
	for(int i=0;i<query_parsed->columns.size();i++)
		cout<<query_parsed->columns[i]<<endl;
	cout<<endl;
	for(int i=0;i<query_parsed->tables.size();i++)
		cout<<query_parsed->tables[i]<<endl;
	cout<<endl;
	for(int i=0;i<query_parsed->comparisions.size();i++)
		cout<<query_parsed->comparisions[i]<<endl;
	cout<<endl;
	for(int i=0;i<query_parsed->operators.size();i++)
		cout<<query_parsed->operators[i]<<endl;
	cout<<endl;
	for(int i=0;i<query_parsed->agg.size();i++)
		cout<<query_parsed->agg[i]<<endl;
	cout<<endl;
}

struct parsed_query * parseit(string input)
{
	struct parsed_query * myquery=new parsed_query;
	int query_len=input.size();
	replace (input.begin(), input.end(), ',' , ' ');
	stringstream input_stream;
	input_stream<<input;
	string token;
	string between="";
	string tok_query="";
	int identifier=-1;
	while(input_stream>>token)
	{
		if(token.compare("SELECT")==0 or token.compare("select")==0 or token.compare("FROM")==0 or token.compare("from")==0 or token.compare("WHERE")==0 or token.compare("where")==0)
		{
			identifier++;
			continue;
		}
		switch(identifier)
		{
			case 0:
				{
					(myquery->columns).push_back(token);
					break;
				}
			case 1:
				{
					(myquery->tables).push_back(token);
					break;
				}
			case 2:
				{
					if(token=="AND")
						token="and";
					else if(token=="OR")
						token="or";
					if(token=="and" or  token=="or")
					{
						(myquery->operators).push_back(token);
						if(between.compare("")!=0)
						{
							string s1="",s2="",s3="";
							int check=0;
							for(int i=0;i<between.size();i++)
							{
								if(between[i]=='<' or between[i]=='>' or between[i]=='=' or between[i]=='!')
									check=1; //opertor check
								else if(check==1 and between[i]!='=')
									check=2; // = came
								if(check==0 and between[i]!=' ')
									s1+=between[i]; //
								if(check==1 and between[i]!=' ')
									s2+=between[i];
								if(check==2 and between[i]!=' ')
									s3+=between[i];
								tok_query=s1+" "+s2+" "+s3;
							}
							(myquery->comparisions).push_back(tok_query);
						}
						between="";
						tok_query="";
						continue;
					}
					between+=token;
					between+=" ";
					break;
				}
			default:
				break;
		}
	}
	if(between!="")
	{
		string s1="",s2="",s3="";
		int check=0;
		for(int i=0;i<between.size();i++)
		{
			if(between[i]=='<' or between[i]=='>' or between[i]=='=' or between[i]=='!')
				check=1; //opertor check
			else if(check==1 and  between[i]!='=')
				check=2; // = came
			if(check==0 and between[i]!=' ')
				s1+=between[i]; //
			if(check==1 and between[i]!=' ')
				s2+=between[i];
			if(check==2 and between[i]!=' ')
				s3+=between[i];
			tok_query=s1+" "+s2+" "+s3;
		}
		(myquery->comparisions).push_back(tok_query);
	}
	return myquery;
}

struct total_db * populate_db(struct total_db * mydb)
{
	dbtype::iterator it;
	int col_counter=1;
	string filename,linedata,int_data;
	for(it=(mydb->data).begin();it!=(mydb->data).end();it++)
	{
		filename="";
		int_data="";
		filename.append(it->first);
		filename.append(".csv");
		ifstream datafile(filename);
		while(datafile>>linedata)
		{
			linedata.erase(remove(linedata.begin(),linedata.end(),'\"'),linedata.end()); //remove all quotes
			replace (linedata.begin(), linedata.end(), ',' , ' ');
			stringstream linestream(linedata);
			col_counter=1;
			while(linestream>>int_data)
			{
				mydb->data[it->first][((mydb->ref)[it->first][col_counter])].push_back(stoi(int_data));
				col_counter++;
			}
		}
		datafile.close();

	}
	return mydb;
}

struct total_db * read_metadata()
{
	ifstream mfile("metadata.txt");
	bool flag=0;// 1 if file is open 0 if file close 
	string metadata,tablename;
	dbtype data;
	dbreftype dataref;
	int col_counter=1;
	while(mfile>>metadata)
	{
		if(flag==1)
		{
			if(metadata[0]=='<')
				flag=0; //file close
			else
			{
				vector < int > tmp;
				tmp.clear();
				data[tablename][metadata]=tmp;
				dataref[tablename][col_counter]=metadata;
				col_counter++;
			}
		}
		else
		{
			if(metadata[0]=='<')
				flag=1;//file open
			mfile>>tablename;
			map < string , vector < int > > temp2;
			temp2.clear();
			data[tablename]=temp2;
			map < int ,string > temp3;
			temp3.clear();
			dataref[tablename]=temp3;
			col_counter=1;
		}
	}
	total_db * mydb=new total_db;
	mydb->data=data;
	mydb->ref=dataref;
	mfile.close();
	return mydb;
}
