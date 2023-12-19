program bde2sql;

{$APPTYPE CONSOLE}
{$R *.res}

uses
  System.SysUtils, Bde.DBTables, System.Classes, Data.DB,
  System.Generics.Collections, Math, StrUtils;

const
  DB_NAME: String = 'xxx';

var
  tblFrom: TTable;
  dbFrom: TDatabase;
  tables: TStringList;
  tableName: String;
  logFile: TextFile;

  arr: Array of String;
  i, c, d: integer;
  fieldDef, newFieldDef: TFieldDef;
  str, prefix: string;
  idx: TIndexDef;

begin
  try
    dbFrom := TDatabase.Create(nil);
//    dbFrom.AliasName := DB_NAME;

    dbFrom.DatabaseName := DB_NAME;
    dbFrom.Name := DB_NAME;
    dbFrom.Directory := 'C:\xxx\';

    tblFrom := TTable.Create(nil);
    tblFrom.DatabaseName := DB_NAME;

    dbFrom.Open;
    tables := TStringList.Create;
    dbFrom.GetTableNames(tables, false);

    AssignFile(logFile, 'dump.sql');
    Rewrite(logFile);

    for tableName in tables do
    begin
      Write(logFile, 'DROP TABLE IF EXISTS ' + tableName + ';' + #10);

      tblFrom.tableName := tableName;
      tblFrom.Open;

      // Tables
      Write(logFile, 'CREATE TABLE ' + tableName + ' (' + #10);
      for c := 0 to tblFrom.FieldDefs.Count - 1 do
      begin
        fieldDef := tblFrom.FieldDefs[c];
        case fieldDef.DataType of
          ftAutoInc:
            str := 'int NOT NULL, CONSTRAINT PK_' + tableName + '_' + fieldDef.Name + ' PRIMARY KEY (' +
              fieldDef.Name + ')';
          ftInteger:
            str := 'int';
          ftString:
            str := 'varchar(' + IntToStr(fieldDef.Size) + ')';
          ftFloat:
            str := 'float';
          ftDate:
            str := 'date';
          ftBoolean:
            str := 'bit';
          ftSmallint:
            str := 'smallint';
          ftMemo:
            str := 'text';
          ftDateTime:
            str := 'timestamp';
          ftTime:
            str := 'time';
          ftBlob:
            str := 'bytea';
        else
          Write(logFile, #10 + 'Campo STRANO: ' + FieldTypeNames[fieldDef.DataType]);
        end;
        if fieldDef.Required then
          str := str + ' NOT NULL';
        if c < tblFrom.FieldDefs.Count - 1 then
          str := str + ',' + #10;
        Write(logFile, #9 + ' ' + fieldDef.Name + ' ' + str);
      end;
      Write(logFile, ');' + #10);

      // Data
      if not tblFrom.IsEmpty then
      begin
        // Write(logFile, 'SET IDENTITY_INSERT dbo.' + tableName + ' ON;' + #10);
        prefix := 'INSERT INTO ' + tableName + ' (';
        for c := 0 to tblFrom.FieldDefs.Count - 1 do
        begin
          prefix := prefix + tblFrom.FieldDefs[c].Name;

          if c < tblFrom.FieldDefs.Count - 1 then
            prefix := prefix + ',';
        end;
        prefix := prefix + ') VALUES ';
        Write(logFile, '-- Records: ' + IntToStr(tblFrom.RecordCount) + #10);

        tblFrom.First;
        d := 0;
        str := '';
        while not tblFrom.Eof do
        begin
          Inc(d);
          str := str + '(';
          for c := 0 to tblFrom.Fields.Count - 1 do
          begin
            if tblFrom.Fields[c].IsNull then
              str := str + 'null'
            else
              case tblFrom.Fields[c].DataType of
                ftString, ftMemo:
                  str := str + #39 + StringReplace(StringReplace(tblFrom.Fields[c].AsString, #39, #39#39, [rfReplaceAll]
                    ), '\', '\\', [rfReplaceAll]) + #39;
                ftAutoInc, ftInteger, ftSmallint, ftFloat:
                  str := str + ' ' + tblFrom.Fields[c].AsString;
                ftBoolean:
                  str := str + ' ' + IfThen(tblFrom.Fields[c].AsBoolean, 'B''1''','B''0''');
                ftDate:
                  str := str + #39 + FormatDateTime('yyyy-mm-dd', tblFrom.Fields[c].AsDateTime) + #39;
                ftDateTime:
                  str := str + #39 + FormatDateTime('yyyy-mm-dd hh:mm:ss', tblFrom.Fields[c].AsDateTime) + #39;
                ftTime:
                  str := str + #39 + FormatDateTime('hh:mm:ss', tblFrom.Fields[c].AsDateTime) + #39;
              else
                // ShowMessage('issues with column: ' + tblFrom.Fields[c].Name);
              end;

            if c < tblFrom.FieldDefs.Count - 1 then
              str := str + ',';
          end;

          if d mod 500 = 0 then
          begin
            Write(logFile, prefix + str + ');' + #10);
            str := '';
          end;

          tblFrom.Next;

          if (d mod 500 <> 0) and (not tblFrom.Eof) then
            str := str + '),' + #10;
        end;

        if d mod 500 <> 0 then
          Write(logFile, prefix + str + ');' + #10);

        // Write(logFile, ';');
        // Write(logFile, 'SET IDENTITY_INSERT dbo.' + tableName + ' OFF;' + #10);
      end;

      // Indexes
      for c := 0 to tblFrom.IndexDefs.Count - 1 do
      begin
        idx := tblFrom.IndexDefs[c];
        if ixPrimary in idx.Options then
          Continue;

        Write(logFile, 'DROP INDEX IF EXISTS "' + idx.Name + '";' + #10);

        str := 'CREATE ';
        if ixUnique in idx.Options then
          str := str + 'UNIQUE ';

        Write(logFile, str + 'INDEX "' + idx.Name + '" ON ' + tableName + ' (' + StringReplace(idx.Fields, ';',
          ',', [rfReplaceAll]) + ');' + #10);
        if ixDescending in idx.Options then
          Write(logFile, '-- ixDescending' + #10);
        // XXX Managed via CI collation
        if ixCaseInsensitive in idx.Options then
        Write(logFile, '-- ixCaseInsensitive' + #10);
        if ixExpression in idx.Options then
          Write(logFile, '-- ixExpression' + #10);
        if ixNonMaintained in idx.Options then
          Write(logFile, '-- ixNonMaintained' + #10);
      end;

      tblFrom.Close;
    end;
    CloseFile(logFile);

  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;

end.
