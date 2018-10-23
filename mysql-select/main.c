#include <stdio.h>
#include <string.h>
#include <mysql/mysql.h>

void bind_bigint(MYSQL_BIND& b_, const long long& val_,
                 my_bool* is_null_, unsigned long* length_,
                 my_bool* error_)
{
  b_.buffer_type = MYSQL_TYPE_LONGLONG;
  b_.buffer = (void *)&val_;
  b_.is_null = is_null_;
  b_.length = length_;
  b_.error = error_;
}

void bind_int(MYSQL_BIND& b_, const int& val_,
              my_bool* is_null_, unsigned long* length_,
              my_bool* error_)
{
  b_.buffer_type = MYSQL_TYPE_LONG;
  b_.buffer = (void *)&val_;
  b_.is_null = is_null_;
  b_.length = length_;
  b_.error = error_;
}

void bind_decimal(MYSQL_BIND& b_, const char* val_,
                  my_bool* is_null_, unsigned long* length_,
                  my_bool* error_)
{
  b_.buffer_type = MYSQL_TYPE_DECIMAL;
  b_.buffer = (void *)val_;
  b_.buffer_length = 20;
  b_.is_null = is_null_;
  b_.length = length_;
  b_.error = error_;
}

void bind_char(MYSQL_BIND& b_, const char* val_,
               unsigned long buffer_length_, my_bool* is_null_,
               unsigned long* length_, my_bool* error_)
{
  b_.buffer_type = MYSQL_TYPE_STRING;
  b_.buffer = (void *)val_;
  b_.buffer_length = buffer_length_;
  b_.is_null = is_null_;
  b_.length = length_;
  b_.error = error_;
}

void bind_varchar(MYSQL_BIND& b_, const char* val_,
                  unsigned long buffer_length_, my_bool* is_null_,
                  unsigned long* length_, my_bool* error_)
{
  b_.buffer_type = MYSQL_TYPE_VAR_STRING;
  b_.buffer = (void *)val_;
  b_.buffer_length = buffer_length_;
  b_.is_null = is_null_;
  b_.length = length_;
  b_.error = error_;
}

void bind_date(MYSQL_BIND& b_, const MYSQL_TIME& val_,
               my_bool* is_null_, unsigned long* length_,
               my_bool* error_)
{
  b_.buffer_type = MYSQL_TYPE_DATE;
  b_.buffer = (void *)&val_;
  b_.buffer_length = 20;
  b_.is_null = is_null_;
  b_.length = length_;
  b_.error = error_;
}

int main() {
  /* mysql connect string */
  const char* host = "192.168.0.106";
  const int port = 6607;
  const char* user = "root";
  const char* passwd = "";
  const char* database = "tpch";

  MYSQL* conn;
  conn = mysql_init(NULL);
  if (conn == NULL) {
    int ret = mysql_errno(conn);
    fprintf(stderr, "ERROR: mysql_init error: %d\n", ret);
    return ret;
  }

  unsigned long client_flag = CLIENT_REMEMBER_OPTIONS;
  if (!mysql_real_connect(conn, host, user, passwd,
                          database, port, NULL, client_flag)) {
    fprintf(stderr,
            "ERROR: mysql_real_connect(host=%s, user=%s, passwd=%s, "
                    "db=%s, port=%d, NULL, CLIENT_REMEMBER_OPTIONS) = %s\n "
                    "database connection fails.\n",
            host, user, passwd, database, port, mysql_error(conn));

    return -1;
  }
  fprintf(stdout, "database connected!\n");

  MYSQL_STMT* stmt = mysql_stmt_init(conn);

  const char* query = "select * from lineitem";

  int err = mysql_stmt_prepare(stmt, query, strlen(query));
  if (err) {
    fprintf(stderr, "ERROR: mysql_stmt_prepare(%s) = %s\n",
            query, mysql_error(conn));
    mysql_stmt_close(stmt);
    return -1;
  }
  fprintf(stdout, "prepare query successful\n");

  MYSQL_RES* prepare_meta_result = mysql_stmt_result_metadata(stmt);
  if (!prepare_meta_result) {
    fprintf(stderr, "ERROR: mysql_stmt_result_metadata(), \
            returned no meta information\n");
    fprintf(stderr, " %s\n", mysql_stmt_error(stmt));
    return -1;
  }

  unsigned int column_count = mysql_num_fields(prepare_meta_result);
  fprintf(stdout, "total columns in select statement: %u\n", column_count);

  if (mysql_stmt_execute(stmt)) {
    fprintf(stderr, "ERROR: mysql stmt execute = %s\n", mysql_stmt_error(stmt));
    return -1;
  }

  MYSQL_BIND bind[16];
  unsigned long length[16];
  my_bool is_null[16];
  my_bool error[16];
  memset(bind, 0, sizeof(bind));
  memset(length, 0, sizeof(length));

  long long val0, val1;
  int val2, val3;
  char *val4 = (char *)malloc(20 * sizeof(char));
  char *val5 = (char *)malloc(20 * sizeof(char));
  char *val6 = (char *)malloc(20 * sizeof(char));
  char *val7 = (char *)malloc(20 * sizeof(char));
  char *val8 = (char *)malloc(1 * sizeof(char));
  char *val9 = (char *)malloc(1 * sizeof(char));
  char *val13 = (char *)malloc(25 * sizeof(char));
  char *val14 = (char *)malloc(10 * sizeof(char));
  char *val15 = (char *)malloc(20 * sizeof(char));
  MYSQL_TIME val10, val11, val12;

  bind_bigint(bind[0], val0, &is_null[0], &length[0], &error[0]);
  bind_bigint(bind[1], val1, &is_null[1], &length[1], &error[1]);
  bind_int(bind[2], val2, &is_null[2], &length[2], &error[2]);
  bind_int(bind[3], val3, &is_null[3], &length[3], &error[3]);
  bind_decimal(bind[4], val4, &is_null[4], &length[4], &error[4]);
  bind_decimal(bind[5], val5, &is_null[5], &length[5], &error[5]);
  bind_decimal(bind[6], val6, &is_null[6], &length[6], &error[6]);
  bind_decimal(bind[7], val7, &is_null[7], &length[7], &error[7]);
  bind_char(bind[8], val8, 1, &is_null[8], &length[8], &error[8]);
  bind_char(bind[9], val9, 1, &is_null[9], &length[9], &error[9]);
  bind_date(bind[10], val10, &is_null[10], &length[10], &error[10]);
  bind_date(bind[11], val11, &is_null[11], &length[11], &error[11]);
  bind_date(bind[12], val12, &is_null[12], &length[12], &error[12]);
  bind_char(bind[13], val13, 25, &is_null[13], &length[13], &error[13]);
  bind_char(bind[14], val14, 10,  &is_null[14], &length[14], &error[14]);
  bind_varchar(bind[15], val15, 20, &is_null[15], &length[15], &error[15]);

  if (mysql_stmt_bind_result(stmt, bind)) {
    fprintf(stderr, "ERROR: bind result error: %s\n", mysql_stmt_error(stmt));
    return -1;
  }

  if (mysql_stmt_store_result(stmt)) {
    fprintf(stderr, "ERROR: stmt store result error: %s\n", mysql_stmt_error(stmt));
    return -1;
  }

  /* fetching data */
  unsigned long long row_count = 0;
  unsigned long long total_length = 0;
  fprintf(stdout, "Fetching results ...\n");
  while (!mysql_stmt_fetch(stmt)) {
    row_count++;
    for (int i = 0; i < 16; ++i) {
      if (!is_null[i]) {
        /* fprintf(stdout, "row: %d, length: %lu \t", i, length[i]); */
        total_length += length[i];
      }
    }
    /* fprintf(stdout, "\n"); */
  }

  fprintf(stdout, "total rows fetched: %llu\n", row_count);
  fprintf(stdout, "total length of data: %llu\n", total_length);
  fprintf(stdout, "average length of data: %llu\n",
          total_length / (row_count ? row_count : 1));

  free(val4);free(val5);free(val6);free(val7);free(val8);free(val9);
  free(val13);free(val14);free(val15);

  mysql_free_result(prepare_meta_result);
  mysql_stmt_close(stmt);
  mysql_close(conn);

  return 0;
}
