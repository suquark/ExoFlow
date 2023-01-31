package wrapper

import (
  "fmt"
  "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
  "github.com/eniac/Beldi/pkg/beldilib"
  "github.com/lithammer/shortuuid"
  "strings"
)

type TxnResult struct {
  TxnId string
  Ok    bool
}

func EOSReadWithRow(tablename string, key string, projection []string, row string) aws.JSONValue {
	var metas []string
	if len(projection) == 0 {
		metas = []string{}
	} else {
		metas = append(projection, "NEXTROW")
	}
	res := beldilib.LibRead(tablename, aws.JSONValue{"K": key, "ROWHASH": row}, metas)
	if nextRow, exists := res["NEXTROW"]; exists {
		return EOSReadWithRow(tablename, key, projection, nextRow.(string))
	}
	for _, column := range beldilib.RESERVED {
		delete(res, column)
	}
	return res
}

func EOSRead(tablename string, key string, projection []string) aws.JSONValue {
	// ReadLog is not in DAAL, Need Optimization Here
	last := beldilib.LastRow(tablename, key)
	if last == "" {
		last = "HEAD"
	}
	return EOSReadWithRow(tablename, key, projection, last)
}

func Read(tablename string, key string) interface{} {
	item := EOSRead(tablename, key, []string{"V"})
	if res, ok := item["V"]; ok {
		return res
	} else {
		return nil
	}
}

func Scan(tablename string) interface{} {
	var res []interface{}
	items := beldilib.LibScan(tablename, []string{"V"},
		expression.AttributeNotExists(expression.Name("NEXTROW")))
	for _, item := range items {
		res = append(res, item["V"])
	}
	return res
}

func TPLRead(env *beldilib.Env, tablename string, key string) (bool, interface{}) {
	if beldilib.Lock(env, tablename, key) {
		return true, Read(tablename, key)
	} else {
		return false, nil
	}
}

func generateTxnEnv(lambdaId string, txnId string, fixed_unique_instance_id string) *beldilib.Env {
  // cid := fmt.Sprintf("%s-%v", env.InstanceId, env.StepNumber)
  // => cid is "logKey" in Beldi paper
  // cidPath := fmt.Sprintf("LOGS.%s", cid)
  // "LOGS.%s" is "RecentWrites[{logKey}]" in Beldi paper
  // So under the context of transaction, we can use
  //   env.InstanceId=lambdaId.TxnId; env.StepNumber = 0
  return &beldilib.Env {
		LambdaId:    lambdaId,
		InstanceId:  shortuuid.New(), // fmt.Sprintf("%s.%s", fixed_unique_instance_id, txnId),
		LogTable:    fmt.Sprintf("%s-log", lambdaId),
		IntentTable: fmt.Sprintf("%s-collector", lambdaId),
		LocalTable:  fmt.Sprintf("%s-local", lambdaId),
		StepNumber:  0,
		Input:       nil,
		TxnId:       txnId,
		Instruction: "",
	}
}

func CommitOrAbortTxn(lambdaId string, txnId string, commit bool) {
  env := generateTxnEnv(lambdaId, txnId, lambdaId + ".commit_or_abort")
	item := EOSRead(env.LocalTable, env.TxnId, []string{})
	// var callees []string
	for k, v := range item {
		if k == "CALLEES" {
			// beldilib.CHECK(mapstructure.Decode(v, &callees))
			continue
		}
		ks := strings.Split(k, "-")
		if len(ks) != 2 {
			continue
		}
		tablename, key := ks[0], ks[1]
		update := map[expression.NameBuilder]expression.OperandBuilder{}
		if commit {
      for kk, vv := range v.(map[string]interface{}) {
        update[expression.Name(kk)] = expression.Value(vv)
      }
		}
		update[expression.Name("HOLDER")] = expression.Value(beldilib.AVAILABLE)
		beldilib.EOSWrite(env, tablename, key, update)
	}
	beldilib.LibDelete(env.LocalTable, aws.JSONValue{"K": env.TxnId, "ROWHASH": "HEAD"})
}

func TxnLock(lambdaId, txnId, tablename string, key string) bool {
  env := generateTxnEnv(lambdaId, txnId, lambdaId + "." + tablename + "." + key)
  return beldilib.Lock(env, tablename, key)
}

func TxnWrite(lambdaId, txnId, tablename string, key string, value aws.JSONValue) {
  env := generateTxnEnv(lambdaId, txnId, lambdaId + "." + tablename + "." + key)
  update := map[expression.NameBuilder]expression.OperandBuilder{}
  tablekey := fmt.Sprintf("%s-%s", tablename, key)
  update[expression.Name(tablekey)] = expression.Value(value)
  beldilib.EOSWrite(env, env.LocalTable, env.TxnId, update)
}
