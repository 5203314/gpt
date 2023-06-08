<?php
namespace Jason\Ccshop\Services;

use DB;
use Cache;
use Jason\Ccshop\Classes\Helper;
use Jason\Ccshop\Models\Moresite;
use Jason\Ccshop\Models\Product;
use Jason\Ccshop\Models\ProductSectionSelectedHistory;
use Jason\Ccshop\Services\ProductService;
use Jason\Ccshop\Models\ProductSection;
use GuzzleHttp\Client;
use Jason\Ccshop\Models\Settings;
use Jason\Ccshop\Classes\Mart\MartClient;
use Carbon\Carbon;
use Jason\Ccshop\Models\OrderStatus;
use Jason\Ccshop\Services\Api\MatomoApiService;
use Jason\Ccshop\Services\Api\MarketApiService;
use Backend\Models\User;
use SerenityNow\Cacheroute\Models\CacheRoute;
use Jason\Ccshop\Models\TraceCode;

class ProductSectionLineService
{
    private static $ip = '172.31.54.111';//172.31.54.111 线上   8.210.222.103 测试

    private static $domain = 'iwaty';

    public static function init()
    {
        $matomo_data_domain = Settings::fetch('matomo_data_unique_domain')?Settings::fetch('matomo_data_unique_domain'):'iwaty,letbes,kiyolook,ladymal';
        if (in_array(Settings::fetch('matomo_data'),explode(',', $matomo_data_domain))) {
            self::$ip = Settings::fetch('matomo_data_interface_ip') ?: '172.31.54.111';
        } else {
            self::$ip = Settings::fetch('matomo_data_interface_ip') ?: '8.210.222.103';
        }
        self::$domain = Settings::fetch('matomo_data') ?: 'iwaty';

        if(get('debug')){
            self::$ip = '8.210.222.103';
            self::$domain =  'iwaty';
        }

    }

    //营销数据获取
    static public function getMarketingData($api='',$params=[]){
        $marketing_domain = 'https://fbadm.neverdown.cc';
        //$marketing_domain = 'http://192.168.1.196:9999';
        self::init();
        $params['dk'] = self::$domain;
        $url = $marketing_domain.$api.'?'.http_build_query($params);
        info('营销数据获取url'.$url);
        $guzzle = new Client([
            'base_uri' => $url,
            'verify'   => false,
            'timeout' => 600.0,

        ]);
        $response = $guzzle->request('GET');
        // 请求的响应主体
        $responseContent = $response->getBody()->getContents();
        $result=json_decode($responseContent,true);
        return $result;
    }
    //go接口数据获取
    static public function getProductsDataFromGo($api,$json_data){
        $guzzle = new Client([
            'verify'   => false,
            'headers'  => [
                'Accept' => 'application/json',
            ]
        ]);

        $api_host = config('app.bigdataapi_host');
        $response = $guzzle->post($api_host.$api, [
            'json' => $json_data
        ]);

        // 解析返回值
        $response = json_decode((string) $response->getBody(), true);
        return $response;

    }
    //产品线列表
    static public function getWorkProductList(){
        $admin_id = intval(post('admin_id',0));
        $submit_time_begin = post('submit_time_begin','');
        $submit_time_end   = post('submit_time_end','');
        $page_size =  intval(post('page_size',20));
        $name = post('name','');
        $id = intval(post('id',0));
        $code = post('code','');
        $putd_ad = intval(post('putd_ad',0));
        $sort = post('sort','');
        $order = post('order','');

        $backendUser = \BackendAuth::getUser();
        // $verfiy = UserService::verfiyAdminPermission(0,$admin_id);

        // if($verfiy['verfiy'] == false){
        //     return ['status'=>301,'data'=>[],'msg'=>'没有权限'];
        // }

        $query =  DB::table('cc_product_sections')->where('is_line',1)->select('id','admin_id','page_abb','name','code','landing_page_link');
        if(!empty($admin_id)){//$verfiy['admin_ids']
            $query->where('admin_id',$admin_id);
        }
        if(!empty($name)){
            $query->where('name',$name);
        }
        if(!empty($code)){
            $query->where('code',$code);
        }
        if(!empty($id)){
            $query->where('id',$id);
        }

        $marketing_data = self::getMarketingData('/marketing/pt/products');
        $marketing_data = $marketing_data['data'] ?? []; 
        if(isset($marketing_data[''])){
            unset($marketing_data['']);
        }

        if(!empty($putd_ad)){
            $page_abb = [];
            foreach($marketing_data as $k=>$v){
                if(!empty($v['activated_ad_count'])){
                    $page_abb[] = $k;
                }

            }
            if($putd_ad == 1){
                $query->whereIn('page_abb',$page_abb);
            }else{
                 $query->whereNotIn('page_abb',$page_abb);
            }
        }



        $items = $query->orderBy('id','desc')->get();
        $admin_users = UserService::getAdminUserNames();
        $data = [];
        $sec_ids  = array_column($items,'id');


        $product_totals = DB::table('cc_product_section_selected')
                            ->select('section_id',DB::raw('count(product_id) as count'))
                            ->whereIn('section_id',$sec_ids)->groupBy('section_id')
                            ->lists('count','section_id');

        //最新审核状态
        $newest_audit_ids = DB::table('cc_section_audit_records')
                            ->where('audit_status','>',0)
                            ->orderBy('id','asc')->whereIn('section_id',$sec_ids)
                            ->lists('id','section_id');
        $newest_audit_records = DB::table('cc_section_audit_records')
                                ->whereIn('id',array_values($newest_audit_ids))
                                ->select('id','section_id','audit_status','submit_time','note')
                                ->get();
        $newest_audit_records = array_column($newest_audit_records,null,'section_id');
        foreach($items as $k=>$v){

            $audit_status = $newest_audit_records[$v->id]->audit_status ?? 0;

            $items[$k]->top_num = $marketing_data[$v->page_abb]['unique_product_count'] ?? 0;
            $items[$k]->putd_ad = !empty($marketing_data[$v->page_abb]['activated_ad_count']) ? 1 : 0;
            $items[$k]->putd_ad_num = $marketing_data[$v->page_abb]['activated_ad_count'] ?? 0;
            $items[$k]->product_total = $product_totals[$v->id] ?? 0;
            $items[$k]->audit_status = $audit_status;
            $items[$k]->submit_time = $newest_audit_records[$v->id]->submit_time ?? '--';
            $items[$k]->audit_id  = $newest_audit_records[$v->id]->id ?? 0;
            $items[$k]->admin_user = $admin_users[$v->admin_id]['name'] ?? '--';
            $items[$k]->note = $newest_audit_records[$v->id]->note ?? '';
          
        }

        if(!empty($sort)){
            if($order == 'desc'){
                $items = collect($items)->sortByDesc($sort)->toArray();
            }else{
                $items = collect($items)->sortBy($sort)->toArray();
            }
            $items = array_values($items);
        }

        if(!empty($submit_time_begin) && !empty($submit_time_end)){
            $submit_time_end = Carbon::parse($submit_time_end)->addDays(1)->toDateString();
            $items = collect($items)->filter(function ($item)use($submit_time_begin,$submit_time_end) {
                if($item->submit_time > $submit_time_begin && $item->submit_time < $submit_time_end){
                    return true;
                }
                return false;
            })->toArray();
            $items = array_values($items);
        }


        $agg = ['product_total'=>0,'top_num'=>0,'putd_ad_num'=>0];

        foreach($items as $item){
            $agg['product_total'] += $item->product_total;
            $agg['top_num'] += $item->top_num;
            $agg['putd_ad_num'] += $item->putd_ad_num;
        }

        $result = Helper::manualPaging($items,['perPage'=>$page_size]);
        $result['status'] = 200;
        $result['agg'] = $agg;
        return $result;
    }

    //单个产品线以天划分数据
    static public function getWorkProductDayList()
    {
        $section_id = post('section_id',0);
        $page_abb = post('page_abb','');
        $data['start'] = post('start','');
        $data['end']   = post('end','');

        if( empty($page_abb) || empty($section_id) ){
            return ['status'=>404,'msg'=>'参数错误'];
        }
        $api = '/marketing/pt/product/' . $page_abb;
        $marketing_data = self::getMarketingData($api ,$data);
        $items = [];
        $total = 0;
        if( $marketing_data['data'] ){

            $product_totals = DB::table('cc_product_section_selected')
                ->where('section_id',$section_id)
                ->count('product_id');

            foreach ($marketing_data['data'] as $key => $value){
                $items[$key]['time'] = $key;
                $items[$key]['top_num'] = $value['unique_product_count'] ?? 0;
                $items[$key]['putd_ad'] = $value['activated_ad_count'] ? 1 : 0;
                $items[$key]['putd_ad_num'] = $value['activated_ad_count'] ?? 0;
                $items[$key]['product_total'] = $product_totals ?? 0;
            }
            $items = collect($items)->sortByDesc('time')->toArray();
            $total = $marketing_data['total'];
        }
        return [
            'status' => 200,
            'total' => $total,
            'data' => $items
        ];
    }

    //更新产品线
    static public function saveWorkProductLine(){
        $params = post();
        $section_id = 0;
        try {
            if(empty($params['name']) || empty($params['code']) || empty($params['page_abb'])){
                return ['status'=>301,'msg'=>'名称，code，缩写不能为空'];
            }
            $now = date('Y-m-d H:i:s');
            $params['page_abb']   = strtoupper($params['page_abb']);

            $verify_letter = preg_match('/^[A-Z0-9\s]+$/',$params['page_abb']);
            if($verify_letter == 0){
                return ['status'=>301,'msg'=>'产品线简称仅支持英文字母和数字'];
            }

            if(!empty($params['id'])){
                $section_id = $params['id'];

                $unique_page_abb = DB::table('cc_product_sections')->where('page_abb',$params['page_abb'])
                                    ->where('id','<>',$section_id)->count();
                if($unique_page_abb){
                    return ['status'=>301,'msg'=>'产品线简称已经占用，不可使用'];
                }


                $line = DB::table('cc_product_sections')->where('id',$section_id)->first();

                $log_fields = ['name','admin_id','code','page_abb'];
                foreach($log_fields as $field){
                    if($params[$field] != $line->$field){
                        self::insertLineOperateLog($section_id,$field,$params[$field],$line->$field);
                    } 
                }

                $params['is_line'] = 1;
                $params['updated_at'] = $now;
                DB::table('cc_product_sections')->where('id',$params['id'])->update($params);
            }else{

                $unique_page_abb = DB::table('cc_product_sections')->where('page_abb',$params['page_abb'])->count();
                if($unique_page_abb){
                    return ['status'=>301,'msg'=>'产品线简称已经占用，不可使用'];
                }

                $params['is_line'] = 1;
                $params['is_enabled'] = 1;
                $params['created_at'] = $now;
                $params['updated_at'] = $now;
                $section_id = DB::table('cc_product_sections')->insertGetId($params);
                self::insertLineOperateLog($section_id,'id',$section_id,0);
            }
            //更新或新增产品线对应页面数据
            AdPageService::saveAdPage('product_line', [
                'page_id' => $section_id,
                'name' => $params['name'] ?? '',
                'name_shorthand' => $params['page_abb'] ?? '',
                'code' => $params['code'] ?? '',
            ]);
        } catch (\Exception $e) {
            return ['status'=>301,'msg'=>$e->getMessage()];
        }
        return ['status'=>200,'msg'=>'操作成功','section_id'=>$section_id];
    }
    /**
     * 删除产品线
     */
    static public function deleteProductLine(){
        $section_id = post('section_id',0);
        $productLine = ProductSection::where('id',$section_id)->first();
        if(!empty($productLine)){
            $backendUser = \BackendAuth::getUser();
            if(empty($backendUser->is_superuser)){
                return ['status'=>301,'msg'=>'没有权限'];
            }
            $delLog = [
                'admin_id' => $backendUser->id,
                'email'    => $backendUser->email,
                'line_info' => $productLine
            ];
            info('删除产品线:'.json_encode($delLog));
            $productLine->delete();
            //假删除产品线页面
            AdPageService::deleteAdPage('product_line', $section_id);
        }
        return ['status'=>200,'msg'=>'删除成功'];
    }
    /**
     * 获取产品线详情
     */
    static public function getLineInfoById(){
        $id = post('id',0);
        $line = DB::table('cc_product_sections')->where('id',$id)->first();
        return ['status'=>200,'data'=>$line];
    }
    //产品线审核列表
    static public function getWorkProductcheckList(){
        $submit_uid = intval(post('submit_uid',0));
        $section_id = intval(post('section_id',0));
        $submit_time_begin = post('submit_time_begin','');
        $submit_time_end   = post('submit_time_end','');
        $audit_uid  = intval(post('audit_uid',0));
        $audit_time_begin = post('audit_time_begin','');
        $audit_time_end = post('audit_time_end','');
        $page_size =  intval(post('page_size',3));
        $audit_status = intval(post('audit_status',0));
        $query = DB::table('cc_section_audit_records')->where('audit_status','>',0)
                ->select('id','section_id','submit_uid','audit_status','submit_time','sort_products','note','optimize_content','audit_uid','audit_time');
        if(!empty($submit_uid)){
            $query->where('submit_uid',$submit_uid);
        }
        if(!empty($submit_time_begin) && !empty($submit_time_end)){
            $query->whereBetween('submit_time',[$submit_time_begin,Carbon::parse($submit_time_end)->addDays(1)->toDateString()]);
        }
        if(!empty($audit_status)){
            $query->where('audit_status',$audit_status);
        }
        if(!empty($section_id)){
            $query->where('section_id',$section_id);
        }
        if(!empty($audit_uid)){
            $query->where('audit_uid',$audit_uid);
        }
        if(!empty($audit_time_begin) && !empty($audit_time_end)){
            $query->whereBetween('audit_time',[$audit_time_begin,Carbon::parse($audit_time_end)->addDays(1)->toDateString()]);
        }
        $items = $query->orderBy('submit_time','desc')->orderBy('id','desc')->paginate($page_size)->toArray();

        $sec_ids = array_values(array_unique(array_column($items['data'],'section_id')));
        $data = [];
        //获取产品线信息
        $sections = DB::table('cc_product_sections')->whereIn('id',$sec_ids)->select('id','admin_id','page_abb','name','code')->get();
        $sections = array_column($sections,null,'id');
        //获取产品线产品信息
        $section_pids = DB::table('cc_product_section_selected')
                        ->whereIn('section_id',$sec_ids)->orderBy('sort','asc')
                        ->select('section_id','product_id')->get();
        foreach($section_pids as $val){
            if(!empty($sections[$val->section_id])){
                $sections[$val->section_id]->pids[]=$val->product_id;
            }
        }
        $admin_users = UserService::getAdminUserNames();

        //广告数据
        $marketing_data = self::getMarketingData('/marketing/pt/products');
        $marketing_data = $marketing_data['data'] ?? [];
        if(isset($marketing_data[''])){
            unset($marketing_data['']);
        }
        foreach($items['data'] as $k=>$v){
            
            $sec_pids = $sections[$v->section_id]->pids ?? [];
            $audit_pids = json_decode($v->sort_products,true) ?? [];
            $admin_id = $sections[$v->section_id]->admin_id ?? 0;
            $page_abb = $sections[$v->section_id]->page_abb ?? '';
            $name = $sections[$v->section_id]->name ?? '';
            $code = $sections[$v->section_id]->code ?? '';
            $optimize_content = $v->optimize_content;
            $optimize_content = json_decode($optimize_content,true);
            $optimize_msg = $optimize_content['msg'] ?? '';
            $add_pids = $optimize_content['add_pids'] ?? [];
            $optimize_idea = $optimize_content['optimize_idea'] ?? [];

            $items['data'][$k]->product_total = count($sec_pids);
            $items['data'][$k]->add_count = count($add_pids);
            $items['data'][$k]->admin_user = $admin_users[$admin_id]['name'] ?? '--';
            $items['data'][$k]->submit_user = $admin_users[$v->submit_uid]['name'] ?? '--';
            $items['data'][$k]->audit_user = $admin_users[$v->audit_uid]['name'] ?? '--';
            $items['data'][$k]->putd_ad = !empty($marketing_data[$page_abb]['activated_ad_count']) ? 1 : 0;
            $items['data'][$k]->page_abb = $page_abb;
            $items['data'][$k]->name = $name;
            $items['data'][$k]->code = $code;
            $items['data'][$k]->optimize_msg = $optimize_msg;
            $items['data'][$k]->optimize_idea = $optimize_idea;
            unset($items['data'][$k]->sort_products);
            unset($items['data'][$k]->optimize_content);
        }
        $items['status'] = 200;
        return $items;
    }
    //获取产品线审核预览
    static public function getLineCheckPreviewData(){
        $audit_id = intval(post('audit_id',0));
        $section_id = intval(post('section_id'),0);
        $page_size = 60;
        $section = DB::table('cc_product_sections')->where('id',$section_id)->first();
        //发布后数据
        $audit_record = DB::table('cc_section_audit_records')->where('id',$audit_id)->where('section_id',$section_id)->first();
        if(empty($audit_record)){
            return ['status'=>404,'msg'=>'参数错误'];
        }
        $optimize_content = json_decode($audit_record->optimize_content,true);
        $publish_after_pids = json_decode($audit_record->sort_products,true) ?? [];
        $publish_before_pids = DB::table('cc_product_section_selected')->where('section_id',$section_id)->orderBy('sort','asc')->lists('product_id');
        if(!empty($publish_before_pids)){
            $publish_before_pids = ProductService::filterProductIdByStatus($publish_before_pids,['instock']);
        }
        $before_result = Helper::manualPaging($publish_before_pids,['perPage'=>$page_size]);
        $after_result = Helper::manualPaging($publish_after_pids,['perPage'=>$page_size]);
        $before_data = self::packageProducts($before_result['data']);
        $after_data = self::packageProducts($after_result['data']);

        $before_keys = array_flip($publish_before_pids);
        $after_keys  = array_flip($publish_after_pids);

        foreach($after_data as $k=>$v){
            $pos = '';
            if(!isset($before_keys[$v['id']])){
                $pos = 'add';
            }elseif($after_keys[$v['id']] < $before_keys[$v['id']]){
                $pos = 'prev';
            }elseif($after_keys[$v['id']] > $before_keys[$v['id']]){
                $pos = 'back';
            }
            $after_data[$k]['position'] = $pos;
        }
        if(count($publish_after_pids) > count($publish_before_pids)){
            $result = $after_result;
        }else{
            $result = $before_result;
        }
        unset($result['data']);

        $after_pids = array_column($after_data,'id');
        $before_pids = array_column($before_data,'id');
        $current_pids = array_values(array_unique(array_merge($before_pids,$after_pids)));
        $matomo_data = self::getProductPageDataFromMatomo(
            $current_pids,
            ['datestart'=>date('Y-m-d',strtotime('-6 days')),'dateend'=>date('Y-m-d'),'entry_url'=>$section->code,'entry_url_keyword'=>'-'.$section->page_abb.'-']
        );

        $matomo_data = array_column($matomo_data['selectData']??[],null,'real_pid');



        foreach($after_data as $k=>$v){
            $after_data[$k]['page_prod_show_qty'] = $matomo_data[$v['id']]['prod_pt'] ?? 0;
            $after_data[$k]['page_prod_click_qty'] = $matomo_data[$v['id']]['prod_clickq'] ?? 0;
            $after_data[$k]['page_unique_visitors'] = $matomo_data[$v['id']]['prod_pt'] ?? 0;
            $after_data[$k]['page_cart_count'] = $matomo_data[$v['id']]['prod_cartt'] ?? 0;
            $after_data[$k]['page_ordered_count'] = $matomo_data[$v['id']]['page_checkoutq'] ?? 0;
            $after_data[$k]['page_total_saled'] = $matomo_data[$v['id']]['total_saled'] ?? 0;
            $after_data[$k]['page_click_rate'] = $matomo_data[$v['id']]['click_rate'] ?? '0%';
            $after_data[$k]['page_cart_rate'] = $matomo_data[$v['id']]['cart_rate'] ?? '0%';
            $after_data[$k]['page_ordered_rate'] = $matomo_data[$v['id']]['order_rate'] ?? '0%';
            $after_data[$k]['page_paid_rate'] = $matomo_data[$v['id']]['paid_rate'] ?? '0%';
        }

        foreach($before_data as $k=>$v){
            $before_data[$k]['page_prod_show_qty'] = $matomo_data[$v['id']]['prod_pt'] ?? 0;
            $before_data[$k]['page_prod_click_qty'] = $matomo_data[$v['id']]['prod_clickq'] ?? 0;
            $before_data[$k]['page_unique_visitors'] = $matomo_data[$v['id']]['prod_pt'] ?? 0;
            $before_data[$k]['page_cart_count'] = $matomo_data[$v['id']]['prod_cartt'] ?? 0;
            $before_data[$k]['page_ordered_count'] = $matomo_data[$v['id']]['page_checkoutq'] ?? 0;
            $before_data[$k]['page_total_saled'] = $matomo_data[$v['id']]['total_saled'] ?? 0;
            $before_data[$k]['page_click_rate'] = $matomo_data[$v['id']]['click_rate'] ?? '0%';
            $before_data[$k]['page_cart_rate'] = $matomo_data[$v['id']]['cart_rate'] ?? '0%';
            $before_data[$k]['page_ordered_rate'] = $matomo_data[$v['id']]['order_rate'] ?? '0%';
            $before_data[$k]['page_paid_rate'] = $matomo_data[$v['id']]['paid_rate'] ?? '0%';
        }

        $result['status'] = 200;
        $result['before_data'] = $before_data;
        $result['after_data'] = $after_data;
        $result['audit_record'] = $audit_record;
        $result['optimize_content'] = $optimize_content;
        return $result;

    }
    static  public function clearCacheBySectionId($sectionId)
    {
        $section = ProductSection::find($sectionId);
        $section->clearSectionCache();
    }
    //产品线提交审核结果
    static public function submitAuditLineCheck(){

        

        $audit_id = intval(post('audit_id'));
        $section_id = intval(post('section_id'));
        $audit = post('audit','');
        $note = post('note','');
        $audit_record = DB::table('cc_section_audit_records')->where('id',$audit_id)->where('section_id',$section_id)->first();
        $section = DB::table('cc_product_sections')->where('id',$section_id)->first();

        $backendUser = \BackendAuth::getUser();

        if(empty($audit_record) || empty($section) || !in_array($audit,['reject','pass'])){
            return ['status'=>404,'msg'=>'参数错误'];
        }
        if($audit_record->audit_status != 1){
            return ['status'=>301,'msg'=>'当前审核单为非待审核状态，不可操作'];
        }

        if($backendUser->id != $audit_record->audit_uid){
            return ['status'=>301,'msg'=>'您非当前审核人，没有当前审核权限'];  
        }


        $now = date('Y-m-d H:i:s');
        if($audit == 'reject'){
            if(empty($note)){
                return ['status'=>301,'msg'=>'请填写驳回原因'];
            }
            //驳回
            DB::table('cc_section_audit_records')->where('id',$audit_record->id)->update(['audit_status'=>3,'updated_at'=>$now,'audit_time'=>$now,'note'=>$note]);
            self::insertLineOperateLog($section_id,'audit_status',3,$audit_record->audit_status);
            return ['status'=>200,'msg'=>'操作成功'];
        }

        $audit_pids = json_decode($audit_record->sort_products,true) ?? [];
        if(empty($audit_pids)){
            return ['status'=>404,'msg'=>'审核单异常'];
        }
        //通过
        $line_pids = DB::table('cc_product_section_selected')->where('section_id',$section_id)->orderBy('sort','asc')->lists('product_id');
        $add_pids = array_values(array_unique(array_diff($audit_pids,$line_pids)));
        $del_pids = array_values(array_unique(array_diff($line_pids,$audit_pids)));

        //删除
        if(!empty($del_pids)){
            //记录删除数据
            ProductSectionSelectedHistory::insertBatch($section_id, $del_pids);

            DB::table('cc_product_section_selected')->where('section_id',$section_id)->whereIn('product_id',$del_pids)->delete();
        }
        
        //添加
        if(!empty($add_pids)){
            $add_data = [];
            foreach($add_pids as $pid){
                $add_data[] = ['section_id'=>$section_id,'product_id'=>$pid,'created_at'=>$now,'updated_at'=>$now,'add_time'=>$now];
            }
            DB::table('cc_product_section_selected')->insert($add_data);
        }
        //更新排序
        $update_sort = [];
        foreach($audit_pids as $k=>$v){
            $update_sort[]=['product_id'=>$v,'sort'=>$k];
        }
        $chunk_sort= collect($update_sort)->chunk(200)->toArray();

        foreach($chunk_sort as $sorts){
            self::batchUpdateLineProductSort($sorts,$section_id);
        }
        $score = intval(post('score',0));
        DB::table('cc_section_audit_records')->where('id',$audit_record->id)->update(['audit_status'=>2,'score'=>$score,'updated_at'=>$now,'audit_time'=>$now]);
        self::insertLineOperateLog($section_id,'audit_status',2,$audit_record->audit_status);
        self::clearCacheBySectionId($section_id);
        //self::clearLineRouteCache($section->code);
        return ['status'=>200,'msg'=>'操作成功'];

    }

    /**
     * 清理产品线路由缓存
     * @param  array  $ids
     */
    public static function clearLineRouteCache($code='')
    {
        // 是否开启缓存路由
        $enabledCacheRoute = Settings::fetch('enabled_cache_route', false);
        if (!$enabledCacheRoute) {
            return;
        }

        // 路由缓存规则
        $cacheRouteRows = CacheRoute::getCacheRouteRows();
        if (empty($cacheRouteRows)) {
            return;
        }

        $lineRow = array_first($cacheRouteRows, function ($key, $item) use($code) {

            return str_contains($item['route_pattern'],$code.'.html');
        });

        if (empty($lineRow)) {
            return;
        }

        $prefix = Cache::getPrefix();
        $cacheKey = $prefix.'SerenityNow.Cacheroute:';
        $redis = CacheRoute::getRedisInstance();
        $id = $lineRow['id'];

        $clearKeys = [];
        $pattern = $cacheKey.$id.':*';
        $allKeysGenerator = RedisService::scanAllKeys($redis, $pattern);
        foreach ($allKeysGenerator as $keys) {
            $clearKeys = array_merge($clearKeys, $keys);
        }

        foreach ($clearKeys as $key) {
            $redis->executeRaw(['UNLINK', $key]);
        }
        return true;
    }

    static public function batchUpdateLineProductSort($sorts,$section_id){
        $pids = array_column($sorts,'product_id');
        if(empty($sorts) || empty($section_id) || empty($pids)){
            return false;
        }
        $sql = 'UPDATE cc_product_section_selected SET sort = ( CASE product_id ';
        foreach($sorts as $k=>$val){
            $sql .= sprintf('WHEN %s THEN %s ',$val['product_id'],$val['sort']);
        }
        $sql .= ' ELSE 99999 END ) WHERE section_id='.$section_id .' AND product_id in ('.implode(",",$pids).')';
        DB::update($sql);
        return true;

    }
    /**
     * 产品线提交排序审核
     */
    static public function submitAuditRecord(){
        //产品线所属人提交审核
        $section_id = intval(post('section_id',0));
        if(empty($section_id)){
            return ['status'=>301,'msg'=>'参数异常'];
        }
        $audit_uid = intval(post('audit_uid',0));
        $optimize_idea = post('optimize_idea','');
        if(empty($optimize_idea)){
            return ['status'=>302,'msg'=>'请填写调整思路'];
        }
        if(empty($audit_uid)){
            return ['status'=>302,'msg'=>'请选择审核人'];
        }
        $backendUser = \BackendAuth::getUser();

        //if($backendUser->id == $audit_uid){
        //    return ['status'=>302,'msg'=>'不可指定自己为审核人'];
        //}


        $section = DB::table('cc_product_sections')->where('id',$section_id)->where('is_line',1)->first();
        if(empty($section)){
            return ['status'=>404,'msg'=>'产品线不存在'];
        }
        $audit_record = DB::table('cc_section_audit_records')->where('section_id',$section_id)->orderBy('id','desc')->first();
        if(empty($audit_record)){
            return ['status'=>300,'msg'=>'排序未作更改，不可提交审核'];
        }
        $now = date('Y-m-d H:i:s');
        $update_pids = json_decode($audit_record->sort_products);
        $update_pids = array_values(array_unique($update_pids));
        if(empty($update_pids)){
            return ['status'=>300,'msg'=>'参数异常，请联系产品'];
        }
        $update_pids = array_map(function($v){
            return intval($v);
        },$update_pids);
        //优化内容
        $line_pids = DB::table('cc_product_section_selected')->orderBy('sort','asc')->where('section_id',$section_id)->lists('product_id');

        if(md5(json_encode($update_pids)) == md5(json_encode($line_pids))){
            return ['status'=>300,'msg'=>'产品线排序未发生变化，提交无效'];
        }

        $add_pids = array_values(array_diff($update_pids,$line_pids));
        $del_pids = array_values(array_diff($line_pids,$update_pids));
        $optimize_type = 0;//只排序了
        if(!empty($add_pids) || !empty($del_pids)){
            $optimize_type = 1;//替换了商品
        } 
        $optimize_content = [
            'msg'=>'新增:'.implode(',',$add_pids).';删除:'.implode(',',$del_pids),
            'add_pids'=>$add_pids,
            'del_pids'=>$del_pids,
            'optimize_idea' => [$optimize_idea],
        ];
        $log_msg = '调整思路:'.$optimize_idea.';'.($optimize_content['msg'] ?? '');
        if($audit_record->audit_status == 1 && $audit_record->audit_uid != $backendUser->id){
            return ['status'=>301,'msg'=>'已提交审核，暂时不可提交'];
        }
        $record_optimize = json_decode($audit_record->optimize_content,true);
        $optimize_content['optimize_idea'] = !empty($record_optimize['optimize_idea']) ? array_merge($record_optimize['optimize_idea'],$optimize_content['optimize_idea']) : $optimize_content['optimize_idea'];
        $update_data = ['submit_uid'=>$backendUser->id,'audit_uid'=>$audit_uid,'audit_status'=>1,'submit_time'=>$now,'updated_at'=>$now,'optimize_type'=>$optimize_type,'optimize_content'=>json_encode($optimize_content)];
        DB::table('cc_section_audit_records')->where('id',$audit_record->id)->update($update_data);

        self::insertLineOperateLog($section->id,'audit_status',1,$audit_record->audit_status,$log_msg);
        return ['status'=>200,'msg'=>'提交成功'];
    }
    //获取产品线预览数据
    static public function getLinePreviewData(){
        $section_id = intval(post('section_id',0));
        //是否有待审核记录，有则展示待审核商品，无则展示产品线数据
        $audit_record = DB::table('cc_section_audit_records')->where('section_id',$section_id)->where('audit_status','>',0)->orderBy('id','desc')->first();
        if(!empty($audit_record) && $audit_record->audit_status == 1){
            $pids = json_decode($audit_record->sort_products,true) ?? [];
        }else{
            $pids = DB::table('cc_product_section_selected')->where('section_id',$section_id)->orderBy('sort','asc')->lists('product_id');
        }
        $result = Helper::manualPaging($pids,['perPage'=>60]);
        $result['data'] = self::packageProducts($result['data']);
        $result['status'] = 200;
        return $result;
    }

    /**
     * 组装产品数据
     */
    static public function packageProducts($pids,$options = []){
        if(empty($pids)){
            return [];
        }
        $products = ProductService::getProductsByIdsFromCache($pids);
        $site_id = intval(post('site_id','-1'));
        $sendData = [
            'per_page' => count($pids),
            'page'     => 1,
            'datestart' => $options['date_start'] ?? date('Y-m-d',strtotime('-6 days')),
            'dateend'   => $options['date_end'] ?? date('Y-m-d'),
            'total_saled_status_id'   => '2,5,12,13',
            'cc_statistical_data_product_id' => implode(',',$pids),
            'campaign_status' => '0,1,2,3'
        ];
        if($site_id != '-1'){
            $sendData['site_id'] = $site_id;
        }

        $matomo_data = MatomoApiService::requestMatomoRealPidData($sendData);
        $matomo_data = json_decode($matomo_data,true);
        $matomo_data = array_column($matomo_data['selectData'] ?? [],null,'real_pid');

        $data = [];
        foreach($products as $k=>$val){
            $data[$k]['id'] = $val['id'];
            $data[$k]['f_thumb'] = $val['f_thumb'];
            $data[$k]['name'] = $val['name'];
            $data[$k]['price'] = $val['price'];
            $data[$k]['prod_show_qty'] = $matomo_data[$val['id']]['prod_pt'] ?? 0;
            $data[$k]['prod_click_qty'] = $matomo_data[$val['id']]['prod_clickq'] ?? 0;
            $data[$k]['cart_count'] = $matomo_data[$val['id']]['prod_cartt'] ?? 0;
            $data[$k]['ordered_count'] = $matomo_data[$val['id']]['page_checkoutq'] ?? 0;
            $data[$k]['total_saled'] = $matomo_data[$val['id']]['total_saled'] ?? 0;
            $data[$k]['click_rate'] = $matomo_data[$val['id']]['click_rate'] ?? '0%';
            $data[$k]['cart_rate'] = $matomo_data[$val['id']]['cart_rate'] ?? '0%';
            $data[$k]['ordered_rate'] = $matomo_data[$val['id']]['order_rate'] ?? '0%';
            $data[$k]['paid_rate'] = $matomo_data[$val['id']]['paid_rate'] ?? '0%';
        }
        return $data;
    }


    /**
     * 拖动排序页面预览数据获取
     */
    static public function getLineSortAreaPreviewData(){
        $section_id = intval(post('section_id',0));
        $per_page = intval(post('size',60));
        $section = DB::table('cc_product_sections')->where('id',$section_id)->where('is_line',1)->first();
        if(empty($section)){
            return ['status'=>404,'msg'=>'参数异常'];
        }

        /**
         * 如果有保存排序更改的审核单，则从审核单获取商品ID,否则从产品线获取数据
         */
        $audit_record = DB::table('cc_section_audit_records')->where('section_id',$section_id)->orderBy('id','desc')->first();

        if(!empty($audit_record) && in_array($audit_record->audit_status,[0,1])){
            $pids = json_decode($audit_record->sort_products,true) ?? [];
        }else{
            $pids = DB::table('cc_product_section_selected')->where('section_id',$section_id)->orderBy('sort','asc')->lists('product_id');
        }
        if(!empty($pids)){
            $pids = ProductService::filterProductIdByStatus($pids,['instock']);
        }
        $result = Helper::manualPaging($pids,['perPage'=>$per_page]);
        $result['data'] = ProductService::getProductsByIdsFromCache($result['data']);
        $result['status'] = 200;
        return $result;
    }

    /**
     * 排序页面产品线排序区商品获取
     */
    static public function getLineSortProduct(){
        $section_id = intval(post('section_id',0));
        $date_start = post('date_start',date('Y-m-d',strtotime('-7 days')));
        $date_end   = post('date_end',date('Y-m-d'));
        $per_page = intval(post('size',60));
        $section = DB::table('cc_product_sections')->where('id',$section_id)->where('is_line',1)->first();
        if(empty($section)){
            return ['status'=>404,'msg'=>'参数异常'];
        }
        /**
         * 如果有保存排序更改或者驳回的审核单，则从审核单获取商品ID,否则从产品线获取数据
         */
        $audit_record = DB::table('cc_section_audit_records')->where('section_id',$section_id)->orderBy('id','desc')->first();

        if(!empty($audit_record) && in_array($audit_record->audit_status,[0,1,3])){
            $pids = json_decode($audit_record->sort_products,true) ?? [];
        }else{
            $pids = DB::table('cc_product_section_selected')->where('section_id',$section_id)->orderBy('sort','asc')->lists('product_id');
        }
        if(!empty($pids)){
            $pids = ProductService::filterProductIdByStatus($pids,['instock']);
        }
        info('000:'.$date_start.$date_end.$section->code.$section->page_abb);
        $result = Helper::manualPaging($pids,['perPage'=>$per_page]);
        $curpage_pids = $result['data'];
        $result['data'] = self::packageProducts($curpage_pids,['date_start'=>$date_start,'date_end'=>$date_end]);

        $matomo_data = self::getProductPageDataFromMatomo(
            $curpage_pids,
            ['datestart'=>$date_start,'dateend'=>$date_end,'entry_url'=>$section->code,'entry_url_keyword'=>'-'.$section->page_abb.'-']
        );

        $matomo_data = array_column($matomo_data['selectData']??[],null,'real_pid');


        $mk_params = [
            'pids'  => $curpage_pids,
            'size'  => count($curpage_pids),
            'start' => $date_start,
            'end'   =>$date_end
        ];
        $marketing_data = self::getMarketingData('/marketing/products',$mk_params);
        $marketing_data = $marketing_data['data'];
        if(!empty($marketing_data)){
            foreach($marketing_data as $k=>$v){
                $marketing_data[$k]['product_id'] = MarketApiService::parsePid($v['ad_name'] ?? '');
            }
            $marketing_data = array_column($marketing_data,null,'product_id');
        }else{
            $marketing_data = [];
        }



        foreach($result['data'] as $k=>$v){
            $result['data'][$k]['page_prod_show_qty'] = $matomo_data[$v['id']]['prod_pt'] ?? 0;
            $result['data'][$k]['page_prod_click_qty'] = $matomo_data[$v['id']]['prod_clickq'] ?? 0;
            $result['data'][$k]['page_unique_visitors'] = $matomo_data[$v['id']]['prod_pt'] ?? 0;
            $result['data'][$k]['page_cart_count'] = $matomo_data[$v['id']]['prod_cartt'] ?? 0;
            $result['data'][$k]['page_ordered_count'] = $matomo_data[$v['id']]['page_checkoutq'] ?? 0;
            $result['data'][$k]['page_total_saled'] = $matomo_data[$v['id']]['total_saled'] ?? 0;
            $result['data'][$k]['page_click_rate'] = $matomo_data[$v['id']]['click_rate'] ?? '0%';
            $result['data'][$k]['page_cart_rate'] = $matomo_data[$v['id']]['cart_rate'] ?? '0%';
            $result['data'][$k]['page_ordered_rate'] = $matomo_data[$v['id']]['order_rate'] ?? '0%';
            $result['data'][$k]['page_paid_rate'] = $matomo_data[$v['id']]['paid_rate'] ?? '0%';
            $result['data'][$k]['effective_status']  = $marketing_data[$v['id']]['effective_status'] ?? '';
        }
        $result['status'] = 200;
        $result['section'] = $section;
        $optimize_idea = [];
        if(!empty($audit_record->optimize_content)){
            $optimize_content = json_decode($audit_record->optimize_content,true);
            $optimize_idea = $optimize_content['optimize_idea'] ?? [];
        }
        $result['optimize_idea'] = $optimize_idea;
        return $result;
    }
    /**
     * 产品线排序调整保存
     */
    static public function saveLineProductSortChange(){
        $page = intval(post('page',0));
        $per_page = intval(post('size',0));
        $sort_pids = post('pids',[]);
        $section_id = intval(post('section_id',0));
        if(is_string($sort_pids)){
            $sort_pids = explode(',',$sort_pids);
        }
        $sort_pids = array_values(array_unique($sort_pids));
        if(empty($page) || empty($per_page) || empty($sort_pids) || empty($section_id)){
            return ['status'=>300,'msg'=>'参数错误'];
        }
        $sort_pids = array_map(function($pid){return intval($pid);},$sort_pids);
        $sort_pids = ProductService::filterProductIdByStatus($sort_pids,['instock']);
        return self::updateAuditRecordProductSort('change',$section_id,$sort_pids,compact('page','per_page'));
    }
    static private function updateAuditRecordProductSort($type = 'change',$section_id,$update_pids,$options=[]){
        $audit_record = DB::table('cc_section_audit_records')->where('section_id',$section_id)->orderBy('id','desc')->first();

        $backendUser = \BackendAuth::getUser();
        if(!empty($audit_record) && $audit_record->audit_status == 1 && $backendUser->id != $audit_record->audit_uid){
            return ['status'=>500,'msg'=>'已提交审核，暂不可更改排序'];
        }



        if(!empty($audit_record) && in_array($audit_record->audit_status,[0,1,3])){
            $record_id = $audit_record->id;
            $pids = json_decode($audit_record->sort_products,true) ?? [];
        }else{
            $pids = DB::table('cc_product_section_selected')->where('section_id',$section_id)->orderBy('sort','asc')->lists('product_id');
        }
        if(!empty($pids)){
            $pids = ProductService::filterProductIdByStatus($pids,['instock']);
        }
        $now = date('Y-m-d H:i:s');
        if(empty($audit_record) || !in_array($audit_record->audit_status,[0,1])){
            //需新建审核单
            $insert_record = ['section_id'=>$section_id,'sort_products'=>json_encode($pids),'created_at'=>$now,'updated_at'=>$now];
            $record_id = DB::table('cc_section_audit_records')->insertGetId($insert_record);
        }
        $page = $options['page'];
        $per_page = $options['per_page'];
        if($type == 'change'){
            $part1 = array_slice($pids,0,$per_page*($page-1));
            $part2 = array_slice($pids,$per_page*$page);
        }elseif($type == 'insert'){
            $row = $options['row'];
            $per_row = $options['per_row'];
            $col = $options['col'];
            $position = $options['position'];
            if(empty($position)){
                $position = $per_row*($row - 1) + $col;
            }
            $slice = $per_page * ($page - 1) + $position;
            $slice = $slice - 1;
            $part1 = array_slice($pids,0,$slice);
            $part2 = array_slice($pids,$slice);
        }else{
            return ['status'=>500,'msg'=>'参数异常'];
        }
        $part1 = array_diff($part1,$update_pids);
        $update_pids = array_values(array_unique(array_merge($part1,$update_pids,$part2)));
        $del_pids = post('del_pids',[]);
        if(!empty($del_pids)){
             $del_pids = ProductService::filterProductIdByStatus($del_pids,['instock']);
             $del_pids = array_values(array_intersect($del_pids,$pids));
        }

        $diff_pids = array_values(array_diff($pids,$update_pids));
        $err_pids = array_values(array_diff($diff_pids,$del_pids));
  
        if(!empty($err_pids)){
            return ['status'=>502,'msg'=>'部分商品可能存在数据异常，'.implode(',',$err_pids)];
        }


        DB::table('cc_section_audit_records')->where('id',$record_id)->update(['sort_products'=>json_encode($update_pids),'updated_at'=>$now,'audit_status'=>0]);
        return ['status'=>200,'msg'=>'保存成功'];

    }
    /**
     * 产品线拖动排序批量插入
     */
    static public function batchInsertPidsLineProduct(){
        $page = intval(post('page',0));
        $per_page = intval(post('size',0));
        $insert_pids = post('pids',[]);
        $section_id = intval(post('section_id',0));
        $row = intval(post('row',1));
        $col = intval(post('col',0));
        $position = intval(post('position',0));
        $per_row = intval(post('per_row',0));

        if(is_string($insert_pids)){
            $insert_pids = explode(',',$insert_pids);
        }
        $insert_pids = array_values(array_unique($insert_pids));
        if(empty($page) || empty($per_page) || empty($insert_pids) || empty($section_id) || empty($per_row)){
            return ['status'=>300,'msg'=>'参数错误'];
        }
        $insert_pids = array_map(function($pid){return intval($pid);},$insert_pids);
        $filter_pids = ProductService::filterProductIdByStatus($insert_pids,['instock']);
        $false_pids = array_values(array_diff($insert_pids,$filter_pids));
        $result = self::updateAuditRecordProductSort('insert',$section_id,$filter_pids,compact('page','per_page','row','col','position','per_row'));
        $msg = '';
        if(!empty($false_pids)){
            $msg = ',其中'.implode(',',$false_pids).'等商品不存在或者不在库，插入失败，其它商品插入成功';
        }
        if($result['status'] == 200){
            $result['msg'] .= $msg;
        }
        return $result;
    }

    /** 
     * 产品线拖动排序撤销更改
     */
    static public function revocationLineProductSort(){
        $section_id = intval(post('section_id',0));
        if(empty($section_id)){
            return ['status'=>300,'msg'=>'参数错误'];
        }
        $audit_record = DB::table('cc_section_audit_records')->where('section_id',$section_id)->orderBy('id','desc')->first();
        if(empty($audit_record) || !in_array($audit_record->audit_status,[0,1])){
            return ['status'=>300,'msg'=>'未作更改，无法撤销'];
        }

        $pids = DB::table('cc_product_section_selected')->where('section_id',$section_id)->orderBy('sort','asc')->lists('product_id');
        if(!empty($pids)){
            $pids = ProductService::filterProductIdByStatus($pids,['instock']);
        }
        
        DB::table('cc_section_audit_records')->where('id',$audit_record->id)
        ->update(['sort_products'=>json_encode($pids),'updated_at'=>date('Y-m-d'),'audit_status'=>0]);
        return ['status'=>200,'msg'=>'撤销成功'];


    }
    //matomo获取排序区商品页面数据表现
    static public function getProductPageDataFromMatomo($pids,$options = []){
        self::init();

        $site_id = intval(post('site_id','-1'));
        $sendData = [
            'per_page' => count($pids),
            'page'     => 1,
            'datestart' => $options['datestart'] ?? '',
            'dateend'   => $options['dateend'] ?? '',
            'entry_url' => $options['entry_url'] ?? '',
            'entry_url_keyword'   => $options['entry_url_keyword'] ?? '',
            'total_saled_status_id'   => '2,5,12,13',
            'cc_statistical_data_product_id' => implode(',',$pids)
        ];
        if($site_id != '-1'){
            $sendData['site_id'] = $site_id;
        }

        $contents = '';
        try {

            $ip = self::$ip;
            $port = 29090;
            $matomoDataInterfaceIp = Settings::fetch('matomo_data_interface_ip', $ip) ?: $ip;
            $matomoDataInterfacePort = (int)Settings::fetch('matomo_data_interface_port', $port) ?: $port;
            $sendData['database'] = 'scz-' . Settings::fetch('matomo_data');
            $mart = new MartClient($matomoDataInterfaceIp, $matomoDataInterfacePort);
            $returnData = $mart->productViewExec('MatomoStatsVirtualPid','getStatsResultWithVirtualPid',['name' => json_encode($sendData)]);
            $contents = $returnData['data']['data'] ?? '';

        } catch (\Exception $exception) {
            $message = $exception->getMessage() . PHP_EOL . $exception->getTraceAsString();
            info($message);
        }
        $result = json_decode($contents,true) ?? [];
        return $result;
    }

    /**
     * 排序页面选品区商品获取
     */
    static public function getLineAreaProduct(){
        //目前排序均为倒序
        $sort = post('sort','');
        $size = intval(post('size',60));
        $page = intval(post('page',1));
        $admin_id = intval(post('admin_id',0));
        $page = 1;
        $size = 500;

        $date_start = post('date_start',date('Y-m-d',strtotime('-7 days')));
        $date_end   = post('date_end',date('Y-m-d'));
        //默认排序为在库时间排序，从数据库获取数据
        $section_id = intval(post('section_id',0));
        $section = DB::table('cc_product_sections')->where('id',$section_id)->first();
        if(empty($section)){
            return ['status'=>404,'msg'=>'产品线不存在'];
        }
        //待添加商品区商品
        $added_ids = DB::table('cc_section_release_preview')->where('user_id',$section->admin_id)->orderBy('id','asc')->lists('product_id');


        $audit_record = DB::table('cc_section_audit_records')->where('section_id',$section_id)->orderBy('id','desc')->first();
        if(!empty($audit_record) && $audit_record->audit_status != 2){
            $line_pids = json_decode($audit_record->sort_products,true) ?? [];
        }else{
            //否则展示产品线数据
            $line_pids = DB::table('cc_product_section_selected')->where('section_id',$section_id)->orderBy('sort','asc')->lists('product_id');
        }
        $exclude_pids = array_values(array_unique(array_merge($added_ids,$line_pids)));
        $size = 600+count($exclude_pids);
        $sort_fields = ['prod_show_qty'=>'prod_pt','prod_click_qty'=>'prod_clickq','click_rate'=>'click_rate','cart_count'=>'prod_cartt','cart_rate'=>'cart_rate','ordered_count'=>'page_checkoutq','ordered_rate'=>'order_rate','total_saled'=>'total_saled','paid_rate'=>'paid_rate'];
        if(isset($sort_fields[$sort])){
            $site_id = intval(post('site_id','-1'));
            $sendData = [
                'per_page' => $size,
                'page'     => 1,
                'datestart' => $date_start,
                'dateend'   => $date_end,
                'total_saled_status_id'   => '2,5,12,13',
                'campaign_status' => '0,1,2,3',
                'order_by_field' => $sort_fields[$sort],
                'orderby' => 'desc'
            ];
            if($site_id != '-1'){
                $sendData['site_id'] = $site_id;
            }
            if(!empty($admin_id)){
                $sendData['admin_id'] = $admin_id;
            }
    
            $matomo_data = MatomoApiService::requestMatomoRealPidData($sendData);
            $matomo_data = json_decode($matomo_data,true);
            $pids = array_column($matomo_data['selectData'] ?? [],'real_pid');
        }else{
            $query = DB::table('cc_products')->where('status','instock');
            if(!empty($admin_id)){
                $query->where('admin_id',$admin_id);
            }
            $pids = $query->orderBy('instock_time','desc')->limit($size)->lists('id');
        }
        if(!empty($exclude_pids)){
            $pids = array_values(array_diff($pids,$exclude_pids));
        }
        $pids = array_slice($pids,0,600);
        $data = Helper::manualPaging($pids,['perPage'=>count($pids)]);
        $data['data'] = self::packageProducts($data['data'],['date_start'=>$date_start,'date_end'=>$date_end]);
        $data['data'] = array_slice($data['data'],0,500);
        $data['per_page'] = $data['to'] = $data['total'] = 500;
        $data['status'] = 200;
        return $data;
    }
    /**
     * 排序页面待添加商品区商品
     */
    static public function getLineAddedProduct(){
        $sort = post('sort','');
        $section_id = intval(post('section_id',0));
        $section = DB::table('cc_product_sections')->where('id',$section_id)->first();
        $date_start = post('date_start',date('Y-m-d',strtotime('-7 days')));
        $date_end   = post('date_end',date('Y-m-d'));
        if(empty($section)){
            return ['status'=>404,'msg'=>'产品线不存在'];
        }
        //待添加商品区商品
        $pids = DB::table('cc_section_release_preview')->where('user_id',$section->admin_id)->orderBy('id','asc')->lists('product_id');

        $audit_record = DB::table('cc_section_audit_records')->where('section_id',$section_id)->orderBy('id','desc')->first();
        if(!empty($audit_record) && $audit_record->audit_status != 2){
            $line_pids = json_decode($audit_record->sort_products,true) ?? [];
        }else{
            //否则展示产品线数据
            $line_pids = DB::table('cc_product_section_selected')->where('section_id',$section_id)->orderBy('sort','asc')->lists('product_id');
        }

        $pids = array_values(array_diff($pids,$line_pids));
        $data = self::packageProducts($pids,['date_start'=>$date_start,'date_end'=>$date_end]);
        if(!empty($sort)){
            $data = collect($data)->sortByDesc($sort)->toArray();
            $data = array_values($data);
        }
        
        return ['status'=>200,'data'=>$data];
    }
    /**
     * 排序页面根据商品ID获取商品数据
     */
    static public function getLineSortProudctDataByPids(){
        $pids = post('pids',[]);
        $date_start = post('date_start',date('Y-m-d',strtotime('-7 days')));
        $date_end   = post('date_end',date('Y-m-d'));
        if(is_string($pids)){
            $pids = explode(',',$pids);
        }
        if(empty($pids)){
            return ['status'=>301,'msg'=>'参数异常','data'=>[]];
        }
        $data = self::packageProducts($pids,['date_start'=>$date_start,'date_end'=>$date_end]);
        return ['status'=>200,'data'=>$data];
    }
    /**
     * 商品产品线matomo数据
     */
    static public function getLineDataMatomo($sendData=[],$handler,$method){
        self::init();
        $contents = [];
        try {

            $ip = self::$ip;
            $port = 49090;
            $mart = new MartClient($ip, $port);
            $returnData = $mart->productViewExec($handler,$method,['name' => json_encode($sendData)]);
            $contents = Helper::utilToArray( $returnData['data']['data'] ?? []);
        } catch (\Exception $exception) {
            $message = $exception->getMessage() . PHP_EOL . $exception->getTraceAsString();
            info($message);
        }
        $result = $contents['list'] ?? [];
        return $result;
    }

    /**
     * 所有渠道产品线报告
     */
    static public function getLineReportAllChannel($lines,$date_start,$date_end,$page_abb=0){
        $lines_facebook = self::getLineReportFacebook($lines,$date_start,$date_end,$page_abb);
        $lines_google = self::getLineReportGoogle($lines,$date_start,$date_end,$page_abb);
        $no_agg_fields = ['id','admin_id','page_abb','name','code','optimize_product_begin_time','optimize_product_end_time','optimize_sort_begin_time','optimize_sort_end_time','section_total_pid_qty'];
        $lines  = [];
        foreach($lines_facebook as $key=>$fbItem){
            $line = $fbItem;
            foreach($fbItem as $field=>$val){
                if(in_array($field,$no_agg_fields)){
                    continue;
                }
                if(is_numeric($line[$field])){
                    $line[$field] += ($lines_google[$key][$field] ?? 0);
                }elseif(is_array($line[$field])){
                    $line[$field] = array_values(array_unique(array_merge($line[$field],$lines_google[$key][$field] ?? [])));
                }
            }

            $line['db_paid_rate'] = !empty($line['old_idvisitor_qty']) ? round($line['db_paid_count']/$line['old_idvisitor_qty'],4) : 0;
            $line['db_paid_3_count_rate'] = !empty($line['visit_more_3_idvisitor_qty']) ? round($line['db_paid_3_count']/$line['visit_more_3_idvisitor_qty'],4) : 0;
            $line['db_paid_5_count_rate'] = !empty($line['visit_more_5_idvisitor_qty']) ? round($line['db_paid_5_count']/$line['visit_more_5_idvisitor_qty'],4) : 0;
            $line['db_paid_7_count_rate'] = !empty($line['visit_more_7_idvisitor_qty']) ? round($line['db_paid_7_count']/$line['visit_more_7_idvisitor_qty'],4) : 0;
            $line['mk_cost_cart'] = !empty($line['mk_cart_count']) ? round($line['mk_spend']/$line['mk_cart_count'],2) : 0;
            $line['mk_cost_register'] = !empty($line['mk_register_count']) ? round($line['mk_spend']/$line['mk_register_count'],2) : 0;
            $line['mk_cost_checkout'] = !empty($line['mk_unique_checkouts']) ? round($line['mk_spend']/$line['mk_unique_checkouts'],2) : 0;
            $line['mk_cost_unique_order'] = !empty($line['mk_unique_ordered_count']) ? round($line['mk_spend']/$line['mk_unique_ordered_count'],2) : 0;
            $line['mk_paid_rate'] = !empty($line['mk_unique_ordered_count']) ? round($line['mk_paid_count']/$line['mk_unique_ordered_count'],4) : 0;
            $line['mk_cost_rate'] = !empty($line['mk_paid_total']) ? round($line['mk_spend']/$line['mk_paid_total'],4) : 0;
            $line['mk_cart_rate'] = !empty($line['mk_unique_visitors']) ? round($line['mk_cart_count']/$line['mk_unique_visitors'],4) : 0;
            $line['mk_order_rate'] = !empty($line['mk_unique_visitors']) ? round($line['mk_unique_ordered_count']/$line['mk_unique_visitors'],4) : 0;
            $line['mk_register_rate'] = !empty($line['mk_unique_visitors'] ) ? round($line['mk_register_count']/$line['mk_unique_visitors'],4) : 0;
            $line['mk_checkout_rate'] = !empty($line['mk_unique_visitors'] ) ? round($line['mk_unique_checkouts']/$line['mk_unique_visitors'],4) : 0;
    
            $line['mk_new_cart_proportion'] = !empty($line['mk_new_visitor'] ) ? round($line['mk_new_cart_count']/$line['mk_new_visitor'],4) : 0;
            $line['mk_old_cart_proportion'] =!empty( $line['mk_old_visitor'] ) ? round($line['mk_old_cart_count']/$line['mk_old_visitor'],4) : 0;
            $line['mk_new_register_proportion'] = !empty($line['mk_new_visitor'] ) ? round($line['mk_new_register_count']/$line['mk_new_visitor'],4) : 0;
            $line['mk_old_register_proportion'] = !empty($line['mk_old_visitor'] ) ? round($line['mk_old_register_count']/$line['mk_old_visitor'],4) : 0;

            $line['mk_new_uc_proportion'] = !empty($line['mk_new_visitor'] ) ? round($line['mk_new_uc_count']/$line['mk_new_visitor'],4) : 0;
            $line['mk_old_uc_proportion'] = !empty($line['mk_old_visitor'] ) ? round($line['mk_old_uc_count']/$line['mk_old_visitor'],4) : 0;
            $line['mk_new_order_proportion'] = !empty($line['mk_new_visitor'] ) ? round($line['mk_new_ordered_count']/$line['mk_new_visitor'],4) : 0;
            $line['mk_old_order_proportion'] = !empty($line['mk_old_visitor'] ) ? round($line['mk_old_ordered_count']/$line['mk_old_visitor'],4) : 0;
    
            $line['mk_new_paid_proportion'] = !empty($line['mk_new_visitor'] ) ? round($line['mk_new_paid_count']/$line['mk_new_visitor'],4) : 0;
            $line['mk_old_paid_proportion'] = !empty($line['mk_old_visitor'] ) ? round($line['mk_old_paid_count']/$line['mk_old_visitor'],4) : 0;

            $line['mk_ad_bounce_rate'] = !empty($line['mk_ga_session']) ? round($line['mk_ga_bound']/$line['mk_ga_session'],4) : 0;

            $line['section_new_pid_qty'] = count($line['section_new_pid_string'] ??[]);
            $line['db_paid_new_product_count'] = count($line['section_paid_new_pids_string'] ??[]);
            $line['section_exposure_pid_qty'] = count($line['section_exposure_pid_string'] ?? []);
            $line['db_paid_product_count'] = count($line['section_paid_pids_string'] ?? []);

            $line['section_exposure_pid_rate'] = !empty($line['section_total_pid_qty']) ? round($line['section_exposure_pid_qty']/$line['section_total_pid_qty'],4) : 0;

            $line['avg_first_cart_time'] = !empty($line['total_idvisitor_qty']) ? round($line['first_cart_time_total']/60/$line['total_idvisitor_qty'],4) : 0;
            $line['avg_first_order_time'] = !empty($line['total_idvisitor_qty']) ? round($line['first_order_time_total']/60/$line['total_idvisitor_qty'],4) : 0;
            $line['avg_visit_pages'] = !empty($line['total_idvisitor_qty']) ? round($line['visit_pages_total']/$line['total_idvisitor_qty'],4) : 0;
            $line['avg_visit_total_time'] = !empty($line['total_idvisitor_qty']) ? round($line['visit_total_time_total']/60/$line['total_idvisitor_qty'],4) : 0;
    
            $line['mk_click_rate'] = $line['mk_impressions'] > 0 ? round($line['mk_unique_clicks']/$line['mk_impressions'],4) : 0;
            $line['mk_atv'] = $line['mk_paid_count'] > 0 ? intval($line['mk_paid_total']/$line['mk_paid_count']) : 0;

            $lines[$key] = $line;
        }
        return $lines;
    }

    /**
     * 获取店铺勾选位
     */
    static public function getBusinessStores(){
        $stores = DB::table('cc_business')->select('id','status','name','check_settings')->get();
        foreach($stores as $store){
            $store->check_settings = json_decode($store->check_settings,true);
            $store->section_code = $store->check_settings['recommend_code'] ?? 'other';
            unset($store->check_settings);
        }
        return $stores;
    }
    /**
     * 商品产品线数据报告
     */

    static public function getLineProductReport($type='line'){
        self::init();
        $lines = DB::table('cc_product_sections')->where('page_abb','<>','')->select('id','admin_id','page_abb','name','code','is_line')->get();
        $lines = json_decode(json_encode($lines),true);
        $lines = array_column($lines,null,'id');
        $stores = self::getBusinessStores();
        $stores = array_column($stores,null,'section_code');
        if($type == 'line'){
            foreach($lines as $k=>$line){
                if(isset($stores[$line['code']]) || empty($line['is_line'])){
                    unset($lines[$k]);
                }
            }
        }else{
            foreach($lines as $k=>$line){
                if(!isset($stores[$line['code']])){
                    unset($lines[$k]);
                    continue;
                }
                $lines[$k]['store_name'] = $stores[$line['code']]->name;
            }
        }

        $date_start = post('date_start',date('Y-m-d',strtotime('-7 days')));
        $date_end   = post('date_end',date('Y-m-d'));

        $channel = post('channel','facebook');
        if($channel == 'facebook'){
            $lines = self::getLineReportFacebook($lines,$date_start,$date_end,0);
        }elseif($channel == 'google'){
           
            $lines = self::getLineReportGoogle($lines,$date_start,$date_end,0);
        }else{
            $lines = self::getLineReportAllChannel($lines,$date_start,$date_end,0);
        }

        $db_date_end = Carbon::parse($date_end)->addDays(1)->toDateString();
        //产品线优化间隔
        $line_audit_records = DB::table('cc_section_audit_records')->whereBetween('submit_time',[$date_start,$db_date_end])
                            ->select('id','section_id','optimize_type','submit_time')->orderBy('submit_time','asc')->get();

        foreach($lines as $k=>$line){
            $lines[$k]['optimize_sort'] = 0;
            $lines[$k]['optimize_product'] = 0;
            $lines[$k]['optimize_sort_total_interval'] = 0;
            $lines[$k]['optimize_product_total_interval'] = 0;
        }
        foreach($line_audit_records as $record){
            if(!isset($lines[$record->section_id])){
                continue;
            }
            if(!empty($record->optimize_type)){
                
                //只调整了排序
                $lines[$record->section_id]['optimize_product'] += 1;
                if(empty($lines[$record->section_id]['optimize_product_begin_time'])){
                    $lines[$record->section_id]['optimize_product_begin_time'] = $record->submit_time;
                }
                $lines[$record->section_id]['optimize_product_end_time'] = $record->submit_time;
            }

            $lines[$record->section_id]['optimize_sort'] += 1;
            if(empty($lines[$record->section_id]['optimize_sort_begin_time'])){
                $lines[$record->section_id]['optimize_sort_begin_time'] = $record->submit_time;
            }
            $lines[$record->section_id]['optimize_sort_end_time'] = $record->submit_time;
        }

        foreach($lines as $key=>$line){
            //产品线优化间隔
            if($line['optimize_sort'] > 1){
                $op_begin = $line['optimize_sort_begin_time'];
                $op_end = $line['optimize_sort_end_time'];
            }else{
                $op_begin = $date_start;
                $op_end   = $db_date_end;
            }
            $time_interval = Carbon::parse($op_begin)->diffInMinutes($op_end);

            $divisor = ($line['optimize_sort'] - 1 > 0) ? $line['optimize_sort'] - 1 : $line['optimize_sort'];
            $lines[$key]['optimize_sort_interval'] = $divisor > 0 ? round($time_interval/$divisor/60,2) : 0;
            if(!empty($line['optimize_sort'])){
                $lines[$key]['optimize_sort_total_interval'] = $time_interval;
            }
            if($line['optimize_product'] > 1){
                $op_begin = $line['optimize_product_begin_time'];
                $op_end = $line['optimize_product_end_time'];
            }else{
                $op_begin = $date_start;
                $op_end   = $db_date_end;
            }
            $time_interval = Carbon::parse($op_begin)->diffInMinutes($op_end);
            $divisor = ($line['optimize_product'] - 1 > 0) ? $line['optimize_product'] - 1 : $line['optimize_product'];
            $lines[$key]['optimize_product_interval'] = $divisor > 0 ? round($time_interval/$divisor/60,2) : 0;

            if(!empty($line['optimize_product'])){
                $lines[$key]['optimize_product_total_interval'] = $time_interval;
            }

        }

        $filters = post('filter_conditions',[]);
        $page_abb = post('page_abb','');
        if(!empty($page_abb)){
            $page_abb = explode(',',$page_abb);
        }else{
            $page_abb  = [];
        }

        if(!empty($filters)){
            $lines = collect($lines)->filter(function($item)use($filters,$page_abb){
                if(!empty($page_abb)){
                    if(!in_array($item['page_abb'],$page_abb)){
                        return false;
                    }
                }
                $flag = true;
                foreach($filters as $filter){
                    $min_value = $filter['min_value'];
                    $max_value = $filter['max_value'];
                    $field = $filter['filter_name'];
                    if(!isset($item[$field])){
                        continue;
                    }
                    if(empty($min_value) && empty($max_value)){
                        continue;
                    }
                    if(empty($min_value)){
                        $min_value = 0;
                    }
                    if(empty($max_value)){
                        $max_value = 9999999;
                    }
                    if($item[$field] >= $min_value && $item[$field] <= $max_value){
                        $flag = true;
                    }else{
                        return false;
                    }
                }
                return $flag;
            })->toArray();
        }

        $agg = self::getLineReportAgg($lines);
        $s = post('s','total_idvisitor_qty');
        $o = strtolower(post('o','desc'));
        if($o == 'desc'){
            $lines = collect($lines)->sortByDesc($s)->toArray();
        }else{
            $lines = collect($lines)->sortBy($s)->toArray();
        }
        $lines = array_values($lines);
        return ['status'=>200,'data'=>$lines,'agg'=>$agg];
    }
    /**
     * 产品线报告获取合计
     */
    static private function getLineReportAgg($lines){
        $no_agg_fields = ['id','admin_id','page_abb','name','code','section_exposure_pid_string','section_new_pid_string','section_paid_pids_string','section_paid_new_pids_string','section_all_pids_string','optimize_product_begin_time','optimize_product_end_time','optimize_sort_begin_time','optimize_sort_end_time','section_add_cart_pids_string'];
        $agg = [];
        foreach($lines as $key=>$line){
            foreach($line as $field=>$val){
                if(in_array($field,$no_agg_fields)){
                    continue;
                }
                if(!isset($agg[$field])){
                    $agg[$field] = 0;
                }
                if(is_numeric($line[$field])){
                    $agg[$field] += $line[$field];
                }
                
            }
        }
        $agg['db_paid_rate'] = !empty($agg['old_idvisitor_qty']) ? round($agg['db_paid_count']/$agg['old_idvisitor_qty'],4) : 0;
        $agg['db_paid_3_count_rate'] = !empty($agg['visit_more_3_idvisitor_qty']) ? round($agg['db_paid_3_count']/$agg['visit_more_3_idvisitor_qty'],4) : 0;
        $agg['db_paid_5_count_rate'] = !empty($agg['visit_more_5_idvisitor_qty']) ? round($agg['db_paid_5_count']/$agg['visit_more_5_idvisitor_qty'],4) : 0;
        $agg['db_paid_7_count_rate'] = !empty($agg['visit_more_7_idvisitor_qty']) ? round($agg['db_paid_7_count']/$agg['visit_more_7_idvisitor_qty'],4) : 0;
        $agg['mk_cost_cart'] = !empty($agg['mk_cart_count']) ? round($agg['mk_spend']/$agg['mk_cart_count'],2) : 0;
        $agg['mk_cost_register'] = !empty($agg['mk_register_count']) ? round($agg['mk_spend']/$agg['mk_register_count'],2) : 0;
        $agg['mk_cost_checkout'] = !empty($agg['mk_unique_checkouts']) ? round($agg['mk_spend']/$agg['mk_unique_checkouts'],2) : 0;
        $agg['mk_cost_unique_order'] = !empty($agg['mk_unique_ordered_count']) ? round($agg['mk_spend']/$agg['mk_unique_ordered_count'],2) : 0;
        $agg['mk_paid_rate'] = !empty($agg['mk_unique_ordered_count']) ? round($agg['mk_paid_count']/$agg['mk_unique_ordered_count'],4) : 0;
        $agg['mk_cost_rate'] = !empty($agg['mk_paid_total']) ? round($agg['mk_spend']/$agg['mk_paid_total'],4) : 0;
        $agg['mk_cart_rate'] = !empty($agg['mk_unique_visitors']) ? round($agg['mk_cart_count']/$agg['mk_unique_visitors'],4) : 0;
        $agg['mk_order_rate'] = !empty($agg['mk_unique_visitors']) ? round($agg['mk_unique_ordered_count']/$agg['mk_unique_visitors'],4) : 0;
        $agg['mk_register_rate'] = !empty($agg['mk_unique_visitors'] ) ? round($agg['mk_register_count']/$agg['mk_unique_visitors'],4) : 0;
        $agg['mk_checkout_rate'] = !empty($agg['mk_unique_visitors'] ) ? round($agg['mk_unique_checkouts']/$agg['mk_unique_visitors'],4) : 0;

        $agg['mk_new_cart_proportion'] = !empty($agg['mk_new_visitor'] ) ? round($agg['mk_new_cart_count']/$agg['mk_new_visitor'],4) : 0;
        $agg['mk_old_cart_proportion'] =!empty( $agg['mk_old_visitor'] ) ? round($agg['mk_old_cart_count']/$agg['mk_old_visitor'],4) : 0;
        $agg['mk_new_register_proportion'] = !empty($agg['mk_new_visitor'] ) ? round($agg['mk_new_register_count']/$agg['mk_new_visitor'],4) : 0;
        $agg['mk_old_register_proportion'] = !empty($agg['mk_old_visitor'] ) ? round($agg['mk_old_register_count']/$agg['mk_old_visitor'],4) : 0;

        $agg['mk_new_uc_proportion'] = !empty($agg['mk_new_visitor'] ) ? round($agg['mk_new_uc_count']/$agg['mk_new_visitor'],4) : 0;
        $agg['mk_old_uc_proportion'] = !empty($agg['mk_old_visitor'] ) ? round($agg['mk_old_uc_count']/$agg['mk_old_visitor'],4) : 0;
        $agg['mk_new_order_proportion'] = !empty($agg['mk_new_visitor'] ) ? round($agg['mk_new_ordered_count']/$agg['mk_new_visitor'],4) : 0;
        $agg['mk_old_order_proportion'] = !empty($agg['mk_old_visitor'] ) ? round($agg['mk_old_ordered_count']/$agg['mk_old_visitor'],4) : 0;

        $agg['mk_new_paid_proportion'] = !empty($agg['mk_new_visitor'] ) ? round($agg['mk_new_paid_count']/$agg['mk_new_visitor'],4) : 0;
        $agg['mk_old_paid_proportion'] = !empty($agg['mk_old_visitor'] ) ? round($agg['mk_old_paid_count']/$agg['mk_old_visitor'],4) : 0;

        $agg['mk_ad_bounce_rate'] = !empty($agg['mk_ga_session']) ? round($agg['mk_ga_bound']/$agg['mk_ga_session'],4) : 0;

        $agg['section_exposure_pid_rate'] = !empty($agg['section_total_pid_qty']) ? round($agg['section_exposure_pid_qty']/$agg['section_total_pid_qty'],4) : 0;

        $agg['avg_first_cart_time'] = !empty($agg['total_idvisitor_qty']) ? round($agg['first_cart_time_total']/60/$agg['total_idvisitor_qty'],4) : 0;
        $agg['avg_first_order_time'] = !empty($agg['total_idvisitor_qty']) ? round($agg['first_order_time_total']/60/$agg['total_idvisitor_qty'],4) : 0;
        $agg['avg_visit_pages'] = !empty($agg['total_idvisitor_qty']) ? round($agg['visit_pages_total']/$agg['total_idvisitor_qty'],4) : 0;
        $agg['avg_visit_total_time'] = !empty($agg['total_idvisitor_qty']) ? round($agg['visit_total_time_total']/60/$agg['total_idvisitor_qty'],4) : 0;

        $agg['mk_click_rate'] = !empty($agg['mk_impressions']) ? round($agg['mk_unique_clicks']/$agg['mk_impressions'],4) : 0;
        $agg['mk_atv'] = !empty($agg['mk_paid_count']) ? intval($agg['mk_paid_total']/$agg['mk_paid_count']) : 0;
        $agg['optimize_sort_interval'] = 0;
        if(!empty($agg['optimize_sort']) && !empty($agg['optimize_sort_total_interval'])){
            $divisor = ($agg['optimize_sort'] - 1 > 0) ? $agg['optimize_sort'] - 1 : $agg['optimize_sort'];
            $agg['optimize_sort_interval'] = $divisor > 0 ? round($agg['optimize_sort_total_interval']/$divisor/60,2) : 0;
        }
        $agg['optimize_product_interval'] = 0;
        if(!empty($agg['optimize_product']) && !empty($agg['optimize_product_total_interval'])){
            $divisor = ($agg['optimize_product'] - 1 > 0) ? $agg['optimize_product'] - 1 : $agg['optimize_product'];
            $agg['optimize_product_interval'] = $divisor > 0 ? round($agg['optimize_product_total_interval']/$divisor/60,2) : 0;
        }
        return $agg;
    }
    /**
     * 商品产品线数据报告，商城站数据
     */
    static private function getLineReportShopSite($lines,$date_start,$date_end,$channel = '',$type=0){

        $db_date_end = Carbon::parse($date_end)->addDays(1)->toDateString();
        $sale_condition = $cart_condition = [];
        if($channel == 'facebook'){
            $sale_condition = ['of.ad_channel'=>'facebook'];
            $cart_condition = ['ad.utm_source'=>'facebook.com'];
        }elseif($channel == 'google'){
            $sale_condition = ['of.ad_channel'=>'Google'];
            $cart_condition = ['ad.utm_source'=>'Google'];
        }
        //筛选下单数
        $sale_status = OrderStatus::saledStatusIds();

        if($type){
            $lines=self::getDbOrdersOnDay($date_start,$db_date_end,$sale_status,$lines);
            $lines=isset($lines['lines']) ? $lines['lines'] :[];
            $new_pids=isset($lines['new_pids']) ? $lines['new_pids'] :[];

        }else{
            $db_orders = DB::table('cc_orders')->whereBetween('created_at',[$date_start,$db_date_end])
                ->whereIn('status_id',$sale_status)
                ->lists('id','uid');
            $new_pids = DB::table('cc_products')->where('instock_time','>',Carbon::now()->subDays(15)->toDateString())->lists('id');
            foreach($db_orders as $uid=>$oid){
                foreach($lines as $key=>$line){
                    if(in_array($uid,$line['visit_more_1_user_id'])){
                        $lines[$key]['db_paid_count'] += 1;
                        $lines[$key]['order_id'][] = $oid;
                        if(in_array($uid,$line['visit_more_3_user_id'])){
                            $lines[$key]['db_paid_3_count'] += 1;
                        }
                        if(in_array($uid,$line['visit_more_5_user_id'])){
                            $lines[$key]['db_paid_5_count'] += 1;
                        }
                        if(in_array($uid,$line['visit_more_7_user_id'])){
                            $lines[$key]['db_paid_7_count'] += 1;
                        }
                        break;
                    }
                }
            }
        }

        foreach($lines as $key=>$line){
            $lines[$key]['db_paid_rate'] = $line['old_idvisitor_qty'] > 0 ? round($line['db_paid_count']/$line['old_idvisitor_qty'],4) : 0;
            $lines[$key]['db_paid_3_count_rate'] = $line['visit_more_3_idvisitor_qty'] > 0 ? round($line['db_paid_3_count']/$line['visit_more_3_idvisitor_qty'],4) : 0;
            $lines[$key]['db_paid_5_count_rate'] = $line['visit_more_5_idvisitor_qty'] > 0 ? round($line['db_paid_5_count']/$line['visit_more_5_idvisitor_qty'],4) : 0;
            $lines[$key]['db_paid_7_count_rate'] = $line['visit_more_7_idvisitor_qty'] > 0 ? round($line['db_paid_7_count']/$line['visit_more_7_idvisitor_qty'],4) : 0;
            unset($lines[$key]['visit_more_1_user_id']);
            unset($lines[$key]['visit_more_3_user_id']);
            unset($lines[$key]['visit_more_5_user_id']);
            unset($lines[$key]['visit_more_7_user_id']);
            $line_pids = DB::table('cc_product_section_selected')->where('section_id',$line['id'])->lists('product_id');
            $lines[$key]['section_total_pid_qty'] = count($line_pids);

            $lines[$key]['section_all_pids_string'] = $line_pids;
            $lines[$key]['section_paid_pids_string'] = [];
            $lines[$key]['db_paid_product_count'] = 0;
            $lines[$key]['db_paid_new_product_count'] = 0;


            
            $lines[$key]['db_new_product_count'] = count(array_intersect($line_pids,$new_pids));
            

            $section_new_pids = $lines[$key]['section_new_pid_string'];
            $section_exposure_pids = $lines[$key]['section_exposure_pid_string'];


            $section_new_pids = array_values(array_intersect($line_pids,$section_new_pids));
            $section_exposure_pids = array_values(array_intersect($line_pids,$section_exposure_pids));

            $lines[$key]['section_exposure_pid_string'] =  $section_exposure_pids;
            $lines[$key]['section_new_pid_string'] =  $section_new_pids;

            $lines[$key]['section_new_pid_qty'] = count($section_new_pids);
            $lines[$key]['section_paid_new_pids_string'] = [];
            $lines[$key]['section_exposure_pid_qty'] = count($section_exposure_pids);

            $lines[$key]['section_add_cart_pids_string'] = [];

            $lines[$key]['section_exposure_pid_rate'] = $lines[$key]['section_total_pid_qty'] > 0 ? round($lines[$key]['section_exposure_pid_qty']/$lines[$key]['section_total_pid_qty'],4) : 0;
        }

        $sale_query = DB::table('cc_order_products as p')->join('cc_orders as o','o.id','=','p.order_id')
                        ->join('cc_ad_tracks as ad','ad.id','=','o.ad_tracks_id')
                        ->join('cc_order_filters as of','of.order_id','=','o.id')
                        ->whereBetween('o.created_at',[$date_start,$db_date_end])->whereIn('o.status_id',$sale_status);
        if(!empty($sale_condition)){
            $sale_query->where($sale_condition);
        }
        if($type){
            $lines=self::getDbSalesPidsOnDay($sale_query,$lines);
        }else{
            $db_sales_pids = $sale_query->distinct('p.product_id','o.ad_tracks_id')
                ->select('p.product_id','ad.utm_campaign')->get();
            //出单商品 使用 -key-匹配
            foreach($db_sales_pids as $key=>$val){
                foreach($lines as $id=>$line){
                    if(strpos($val->utm_campaign,'-'.$line['page_abb'].'-')){
                        if(in_array($val->product_id,$line['section_all_pids_string']) && !in_array($val->product_id,$lines[$id]['section_paid_pids_string'])){
                            $lines[$id]['section_paid_pids_string'][] = $val->product_id;
                            $lines[$id]['db_paid_product_count'] += 1;
                            //新品出单款数
                            if(in_array($val->product_id,$line['section_new_pid_string']) && !in_array($val->product_id,$lines[$id]['section_paid_new_pids_string'])){
                                $lines[$id]['section_paid_new_pids_string'][] = $val->product_id;
                                $lines[$id]['db_paid_new_product_count'] += 1;
                            }
                        }
                        break;
                    }
                }
            }

        }

        $cart_query = DB::table('cc_shopping_cart_values as p')
            ->join('cc_ad_tracks as ad','ad.id','=','p.ad_tracks_id')
            ->whereBetween('p.created_at',[$date_start,$db_date_end]);
        if(!empty($cart_condition)){
            $cart_query->where($cart_condition);
        }
        if($type){
            $lines=self::getAddCartPidsOnDay($cart_query,$lines);
        }else{
            $add_cart_pids = $cart_query->distinct('p.product_id','p.ad_tracks_id')->select('p.product_id','ad.utm_campaign')->get();
            //加购商品 使用 -key-匹配
            foreach($add_cart_pids as $key=>$val){
                foreach($lines as $id=>$line){
                    if(strpos($val->utm_campaign,'-'.$line['page_abb'].'-')){
                        if(!in_array($val->product_id,$lines[$id]['section_add_cart_pids_string'])){
                            $lines[$id]['section_add_cart_pids_string'][] = $val->product_id;
                        }
                        break;
                    }
                }
            }
        }
        return $lines;
    }
    public static function getAddCartPidsOnDay($cart_query,$lines){
        $add_cart_pids = $cart_query->distinct('p.product_id','p.ad_tracks_id')
            ->select('p.product_id','ad.utm_campaign', DB::raw('DATE(p.created_at) as date'))
            ->get();
        //加购商品 使用 -key-匹配
        foreach($add_cart_pids as $key=>$val){
            $date = $val->date;
            foreach($lines as $id=>$line){
                if(isset($line['page_abb'])){
                    if(strpos($val->utm_campaign,'-'.$line['page_abb'].'-')){
                        if(!in_array($val->product_id,$lines[$id]['section_add_cart_pids_string'])){
                            $lines[$date]['section_add_cart_pids_string'][] = $val->product_id;
                        }
                        break;
                    }
                }
            }
        }
        return $lines;
    }
    public static function getDbOrdersOnDay($date_start,$db_date_end,$sale_status,$lines){
//        $db_orders = DB::table('cc_orders')
//            ->selectRaw('DATE(created_at) as order_date, COUNT(*) as order_count,id,uid')
//            ->whereBetween('created_at', [$date_start, $db_date_end])
//            ->whereIn('status_id', $sale_status)
//            ->groupBy('order_date')
//            ->get();
        $db_orders = DB::table('cc_orders')
            ->select(DB::raw('DATE(created_at) as order_date'), DB::raw('COUNT(*) as order_count'), 'id', DB::raw('GROUP_CONCAT(uid) as user_id'))
            ->whereBetween('created_at', [$date_start, $db_date_end])
            ->whereIn('status_id', $sale_status)
            ->groupBy('order_date')
            ->get();
        $db_orders=json_decode(json_encode($db_orders),true);
        $arr=[];
        if($db_orders){
            foreach ($db_orders as $value){
                if(isset($value['order_date'])){
                    $arr[$value['order_date']]=[
                              'order_count'=>$value['order_count']  ?? 0,
                              'user_id'=>$value['user_id']  ? explode(',',$value['user_id']) :[],
                              'id'=>$value['id'] ?? 0
                    ];
                }
            }
        }
        $db_orders=$arr;
        $new_pids = DB::table('cc_products')
            ->where('instock_time','>',Carbon::now()
                ->subDays(15)->toDateString())->lists('id');

        foreach($lines as $key=>$line) {
            if(isset($db_orders[$key])){
                info('visit_more_1_user_id:'.$key.':'.json_encode($line['visit_more_1_user_id']));
                info('user_id:'.$key.':'.json_encode($db_orders[$key]['user_id']));
                $lines[$key]['db_paid_count']=count(array_intersect($line['visit_more_1_user_id'],$db_orders[$key]['user_id'])) ?? 0;
                $lines[$key]['order_id'][] = $db_orders[$key]['id'];
                $lines[$key]['db_paid_3_count']=count(array_intersect($line['visit_more_3_user_id'],$db_orders[$key]['user_id'])) ?? 0;
                $lines[$key]['db_paid_5_count']=count(array_intersect($line['visit_more_5_user_id'],$db_orders[$key]['user_id'])) ?? 0;
                $lines[$key]['db_paid_7_count']=count(array_intersect($line['visit_more_7_user_id'],$db_orders[$key]['user_id'])) ?? 0;
            }
        }

        foreach($lines as $key=>$line){
            $lines[$key]['db_paid_rate'] = $line['old_idvisitor_qty'] > 0 ? round($line['db_paid_count']/$line['old_idvisitor_qty'],4) : 0;
            $lines[$key]['db_paid_3_count_rate'] = $line['visit_more_3_idvisitor_qty'] > 0 ? round($line['db_paid_3_count']/$line['visit_more_3_idvisitor_qty'],4) : 0;
            $lines[$key]['db_paid_5_count_rate'] = $line['visit_more_5_idvisitor_qty'] > 0 ? round($line['db_paid_5_count']/$line['visit_more_5_idvisitor_qty'],4) : 0;
            $lines[$key]['db_paid_7_count_rate'] = $line['visit_more_7_idvisitor_qty'] > 0 ? round($line['db_paid_7_count']/$line['visit_more_7_idvisitor_qty'],4) : 0;
            unset($lines[$key]['visit_more_1_user_id']);
            unset($lines[$key]['visit_more_3_user_id']);
            unset($lines[$key]['visit_more_5_user_id']);
            unset($lines[$key]['visit_more_7_user_id']);
            $line_pids = DB::table('cc_product_section_selected')->where('section_id',$line['id'])->lists('product_id');
            $lines[$key]['section_total_pid_qty'] = count($line_pids);
            $lines[$key]['section_all_pids_string'] = $line_pids;
            $lines[$key]['section_paid_pids_string'] = [];
            $lines[$key]['db_paid_product_count'] = 0;
            $lines[$key]['db_paid_new_product_count'] = 0;
            $lines[$key]['db_new_product_count'] = count(array_intersect($line_pids,$new_pids));
            $section_new_pids = $lines[$key]['section_new_pid_string'];
            $section_exposure_pids = $lines[$key]['section_exposure_pid_string'];
            $section_new_pids = array_values(array_intersect($line_pids,$section_new_pids));
            $section_exposure_pids = array_values(array_intersect($line_pids,$section_exposure_pids));
            $lines[$key]['section_exposure_pid_string'] =  $section_exposure_pids;
            $lines[$key]['section_new_pid_string'] =  $section_new_pids;
            $lines[$key]['section_new_pid_qty'] = count($section_new_pids);
            $lines[$key]['section_paid_new_pids_string'] = [];
            $lines[$key]['section_exposure_pid_qty'] = count($section_exposure_pids);
            $lines[$key]['section_add_cart_pids_string'] = [];
            $lines[$key]['section_exposure_pid_rate'] = $lines[$key]['section_total_pid_qty'] > 0 ? round($lines[$key]['section_exposure_pid_qty']/$lines[$key]['section_total_pid_qty'],4) : 0;
        }
        return  array('lines' => $lines, 'new_pids' => $new_pids);
    }

    public static function getDbSalesPidsOnDay($sale_query,$lines){
        $db_sales_pids = $sale_query->distinct('p.product_id','o.ad_tracks_id')
            ->select('p.product_id','ad.utm_campaign', DB::raw('DATE(o.created_at) as date'))
            ->get();
        //出单商品 使用 -key-匹配
        foreach($db_sales_pids as $key=>$val){
            $date = $val->date;
            foreach($lines as $id=>$line){
                if(isset($line['page_abb'])) {
                    if (strpos($val->utm_campaign, '-' . $line['page_abb'] . '-')) {
                        if (in_array($val->product_id, $line['section_all_pids_string']) && !in_array($val->product_id,
                                $lines[$id]['section_paid_pids_string'])) {
                            $lines[$date]['section_paid_pids_string'][] = $val->product_id;
                            $lines[$date]['db_paid_product_count'] += 1;
                            //新品出单款数
                            if (in_array($val->product_id,
                                    $line['section_new_pid_string']) && !in_array($val->product_id,
                                    $lines[$id]['section_paid_new_pids_string'])) {
                                $lines[$date]['section_paid_new_pids_string'][] = $val->product_id;
                                $lines[$date]['db_paid_new_product_count'] += 1;
                            }
                        }
                        break;
                    }
                }
            }
        }
        return $lines;
    }

    /**
     * 商品产品线数据报告，matomo数据
     */
    static private function getLineReportMatomo($lines,$date_start,$date_end,$channel='',$type=0){
        $sendData = [
            'date_start' => $date_start,
            'date_end'   => $date_end,
            'database_name' => 'scz-' . self::$domain,
            'matomo_site_id' => TraceCode::fetch('matomo_site_id') ?: 66,
            'new_pid_last_days'=>15,
            'all_pid_exposure_qty_low'=>200,
            'new_pid_exposure_qty_low'=>50,
            'new_pid_exposure_qty_upper'=>2000
        ];

        if(!empty($channel)){
            $sendData['ad_source'] = $channel;
        }
        if($type){
            $sendData['section_id'] =current($lines)['id'];
            $sendData['page_abb'] =current($lines)['page_abb'];
        }

        if(empty($type)){
            $matomo_data = self::getLineDataMatomo($sendData,'Project2Handler','getPidLineReport');
            $matomo_data = array_column($matomo_data,null,'section_id');
        }else{
            $matomo_data = self::getLineDataMatomo($sendData,'Project2Handler','getPidLineReportSingle');
            $res_matomo=[];
            if($matomo_data){
                foreach ($matomo_data as $value){
                    //if($value['section_id']!=current($lines)['id']) continue;
                    if(isset($value['visit_date'])){
                       // if(isset($res_matomo[$value['visit_date']])){
                            $res_matomo[$value['visit_date']]=$value;
                      //  }
                    }
                }
            }
        }



        foreach($lines as $key=>$line){
            if(empty($type)){
                $page_matomo = $matomo_data[$line['id']] ?? [];
            }else{
                $page_matomo = $res_matomo[$key] ?? [];
            }
            $lines[$key]['visit_more_1_user_id'] = explode(',',$page_matomo['visit_more_1_user_id'] ?? '');
            $lines[$key]['old_idvisitor_qty'] = $page_matomo['old_idvisitor_qty'] ?? 0;
            $lines[$key]['db_paid_count'] = 0;
            $lines[$key]['visit_more_3_user_id'] = explode(',',$page_matomo['visit_more_3_user_id'] ?? '');
            $lines[$key]['visit_more_3_idvisitor_qty'] =  $page_matomo['visit_more_3_idvisitor_qty'] ?? 0;
            $lines[$key]['db_paid_3_count'] = 0;
            $lines[$key]['visit_more_5_user_id'] = explode(',',$page_matomo['visit_more_5_user_id'] ?? '');
            $lines[$key]['visit_more_5_idvisitor_qty'] = $page_matomo['visit_more_5_idvisitor_qty'] ?? 0;
            $lines[$key]['db_paid_5_count'] = 0;
            $lines[$key]['visit_more_7_user_id'] = explode(',',$page_matomo['visit_more_7_user_id'] ?? '');
            $lines[$key]['visit_more_7_idvisitor_qty'] =  $page_matomo['visit_more_7_idvisitor_qty'] ?? 0;
            $lines[$key]['db_paid_7_count'] = 0;

            $lines[$key]['total_idvisitor_qty'] = $page_matomo['total_idvisitor_qty'] ?? 0;
            $lines[$key]['section_new_pid_qty'] = $page_matomo['section_new_pid_qty'] ?? 0;

            $lines[$key]['avg_first_order_time'] = round(($page_matomo['avg_first_order_time'] ?? 0)/60,2);

            $lines[$key]['avg_first_cart_time'] = round(($page_matomo['avg_first_cart_time'] ?? 0)/60,2);
            $lines[$key]['avg_visit_pages'] = $page_matomo['avg_visit_pages'] ?? 0;
            $lines[$key]['avg_visit_total_time'] = round(($page_matomo['avg_visit_total_time'] ?? 0)/60,2);


            $lines[$key]['section_exposure_pid_qty'] = $page_matomo['section_exposure_pid_qty'] ?? 0;
            $lines[$key]['section_total_pid_qty'] = $page_matomo['section_total_pid_qty'] ?? 0;

            $lines[$key]['section_exposure_pid_string'] =  explode(',',$page_matomo['section_exposure_pid_string'] ?? '');
            $lines[$key]['section_new_pid_string'] =  explode(',',$page_matomo['section_new_pid_string'] ?? '');

            $lines[$key]['visit_pages_total'] = (($page_matomo['avg_visit_pages'] ?? 0)*($page_matomo['total_idvisitor_qty'] ?? 0));
            $lines[$key]['first_cart_time_total'] = (($page_matomo['avg_first_cart_time'] ?? 0)*($page_matomo['total_idvisitor_qty'] ?? 0));
            $lines[$key]['first_order_time_total'] = (($page_matomo['avg_first_order_time'] ?? 0)*($page_matomo['total_idvisitor_qty'] ?? 0));
            $lines[$key]['visit_total_time_total'] = (($page_matomo['avg_visit_total_time'] ?? 0)*($page_matomo['total_idvisitor_qty'] ?? 0));
        }

        return $lines;
    }
    /**
     * facebook产品线营销数据
     */
    static private function getLineReportFacebook($lines,$date_start,$date_end,$page_abb=0){
        //获取facebook营销数据
        $pts = array_column($lines,'page_abb');
        $pts = implode(',',$pts);
        if(empty($page_abb)){
            $params = ['start'=>$date_start,'end'=>$date_end,'pts'=>$pts];
            $cacheKey = 'marketing_data_facebook.'.md5(json_encode($params));
        }else{
            $params = ['start'=>$date_start,'end'=>$date_end];
            $cacheKey = 'marketing_data_facebook.'.md5(json_encode($params)).$page_abb;
        }


        $expires = 60;
         $lines =  \Cache::tags('product_section_line_v3')->remember($cacheKey,$expires, function () use ($lines,$date_start,$date_end,$page_abb,$params) {

        if(empty($page_abb)){
            $marketing_data = self::getMarketingData('/marketing/pts',$params);
            $marketing_data = array_column($marketing_data['data'] ?? [] ,null,'pt');
        }else{
            $marketing_data = self::getMarketingData("/marketing/pt/".$page_abb,$params);
            $lines_on_day=$lines;
            $lines=$marketing_data['data'];
        }

        if( self::$domain == 'sowelook' ){
            foreach ($marketing_data as $key => $value){
                $marketing_data[$key]['spend_jpy'] = $value['spend'];
            }
        }

        //营销数据需要的字段
        $marketing_fields = ['product_num','all_num','active_num','unique_visitors','cart_count','new_cart_count','old_cart_count','new_register_count','old_register_count','new_ordered_count','old_ordered_count','new_uc_count','old_uc_count','cost_cart','register_count','cost_register','unique_checkouts','cost_checkout','unique_ordered_count','cost_unique_order','paid_count','paid_total','ga_session','ga_bound','new_visitor','old_visitor','new_paid_count','old_paid_count','unique_clicks','impressions'];
        $marketing_rates = ['new_cart_proportion','old_cart_proportion','new_order_proportion','old_order_proportion','new_register_proportion','old_register_proportion','new_uc_proportion','old_uc_proportion','marketing_proportion','new_paid_proportion','old_paid_proportion','ad_bounce_rate','paid_rate','cart_proportion','order_proportion','register_proportion','checkout_proportion','ctr','atv'];
        $diff_fields = [
            'spend' => 'spend_jpy'
        ];
        $marketing_fields = array_merge($marketing_fields,$marketing_rates,array_values($diff_fields));

        
        foreach($lines as $key=>$line){
            if($page_abb){
                $mk_page_data=$line;
            }else{
                $mk_page_data = $marketing_data[$line['page_abb']] ?? [];
            }
            foreach($marketing_fields as $field){
                $mk_field = 'mk_'.$field;
                $val = $mk_page_data[$field] ?? 0;
                if($field == 'atv'){
                    $val = intval($val);
                }elseif(in_array($field,$marketing_rates)){
                    $val = round($val/100,4);
                }
                $lines[$key][$mk_field] = $val;
            }

            foreach($diff_fields as $k=>$v){
                $lines[$key]['mk_'.$k] = $lines[$key]['mk_'.$v];
                unset($lines[$key]['mk_'.$v]);
            }

            if($page_abb){
                $lines[$key]['id'] = current($lines_on_day)['id'];
                $lines[$key]['admin_id'] = current($lines_on_day)['admin_id'];
                $lines[$key]['page_abb'] = current($lines_on_day)['page_abb'];
                $lines[$key]['code'] = current($lines_on_day)['code'];
                $lines[$key]['is_line'] = current($lines_on_day)['is_line'];
                $lines[$key]['date'] = $key;
            }

            $lines[$key]['mk_cost_rate'] = $lines[$key]['mk_marketing_proportion'];
            $lines[$key]['mk_cart_rate'] = $lines[$key]['mk_cart_proportion'];
            $lines[$key]['mk_order_rate'] = $lines[$key]['mk_order_proportion'];
            $lines[$key]['mk_register_rate'] = $lines[$key]['mk_register_proportion'];
            $lines[$key]['mk_checkout_rate'] = $lines[$key]['mk_checkout_proportion'];
            $lines[$key]['mk_click_rate'] = $lines[$key]['mk_ctr'];
        }

     
        $lines = self::getLineReportMatomo($lines,$date_start,$date_end,'facebook',$page_abb);
        $lines = self::getLineReportShopSite($lines,$date_start,$date_end,'facebook',$page_abb);
      
        return $lines;
          });
        return $lines;

    }

    /**
     * google产品线营销数据
     */
    static private function getLineReportGoogle($lines,$date_start,$date_end,$page_abb=0){
        $view_id = Settings::fetch('ga_view_id');
        $json_data = [
            'view_id'       =>$view_id,
            'date_start'    =>$date_start,
            'date_end'      =>$date_end,
        ];
        if($page_abb){
            $json_data['product_line_id']=current($lines)['id'];
            $cacheKey = 'marketing_data_google.'.md5(json_encode($json_data)).$page_abb;
        }else{
            $cacheKey = 'marketing_data_google.'.md5(json_encode($json_data));
        }

        $expires = 60;
        $lines = \Cache::tags('product_section_line_v3')->remember($cacheKey,$expires, function () use ($lines,$date_start,$date_end,$page_abb,$json_data){
        if($page_abb){
            $api = '/google/productlines/daily';
            $marketing_data = MarketingGoogleService::getResponseFromGo($api,$json_data);
            $arr=$lines_on_day=[];
            if(isset($marketing_data['data']['list'])){
                foreach ($marketing_data['data']['list'] as $key=>$value){
                    if($value['product_line_id']!=current($lines)['id']) continue;
                    if(isset($arr[$value['date']])){
                        $arr[$value['date']]=$value;
                    }
                }
            }
            $lines_on_day=$lines;
            $lines=$arr;
        }else{
            $api = '/google/productlines';
            $marketing_data = MarketingGoogleService::getResponseFromGo($api,$json_data);
            $marketing_data = array_column($marketing_data['data']['list'] ?? [],null,'product_line_id');
        }

            $marketing_fields = [
            'unique_visitors','cart_count','new_cart_count','old_cart_count','new_register_count','old_register_count','new_ordered_count','old_ordered_count','new_uc_count','old_uc_count','new_paid_count','old_paid_count','cost_cart','register_count','cost_register','unique_checkouts','cost_checkout','unique_ordered_count','cost_unique_order','paid_count','paid_total','ga_session','ga_bound','new_visitor','old_visitor','impressions'];
            $marketing_rates = ['new_cart_proportion','old_cart_proportion','new_order_proportion','old_order_proportion','new_register_proportion','old_register_proportion','new_uc_proportion','old_uc_proportion','new_paid_proportion','old_paid_proportion','marketing_proportion','paid_rate','cart_proportion','order_proportion','register_proportion','checkout_proportion','atv'];

            $diff_fields = [
                'spend'=>'cost',
                'active_num'=>'enabled_count',
                'paused_num' => 'paused_count',
                'unique_clicks'=>'clicks',
    
            ];
            $diff_rates = [
                'ad_bounce_rate' => 'ga_bound_rate',
                'ctr'=>'clicks_rate'
            ];

        $marketing_fields = array_merge($marketing_fields,$marketing_rates,array_values($diff_fields),array_values($diff_rates));
        foreach($lines as $key=>$line){
            if(empty($page_abb)){
                $mk_page_data = $marketing_data[$line['page_abb']] ?? [];
            }else{
                $mk_page_data=$line;
            }
            foreach($marketing_fields as $field){
                $mk_field = 'mk_'.$field;
                $val = $mk_page_data[$field] ?? 0;
                if($field == 'atv'){
                    $val = intval($val);
                }
                $lines[$key][$mk_field] = $val;
            }
            foreach(array_merge($diff_fields,$diff_rates) as $k=>$v){
                $lines[$key]['mk_'.$k] = $lines[$key]['mk_'.$v];
                unset($lines[$key]['mk_'.$v]);
            }

            $lines[$key]['mk_all_num'] = $lines[$key]['mk_active_num'] + $lines[$key]['mk_paused_num'];
            $lines[$key]['mk_cost_rate'] = $lines[$key]['mk_marketing_proportion'];
            $lines[$key]['mk_cart_rate'] = $lines[$key]['mk_cart_proportion'];
            $lines[$key]['mk_order_rate'] = $lines[$key]['mk_order_proportion'];
            $lines[$key]['mk_register_rate'] = $lines[$key]['mk_register_proportion'];
            $lines[$key]['mk_checkout_rate'] = $lines[$key]['mk_checkout_proportion'];
            $lines[$key]['mk_click_rate'] = $lines[$key]['mk_ctr'];
            if($page_abb){
                $lines[$key]['id'] = current($lines_on_day)['id'];
                $lines[$key]['admin_id'] = current($lines_on_day)['admin_id'];
                $lines[$key]['page_abb'] = current($lines_on_day)['page_abb'];
                $lines[$key]['code'] = current($lines_on_day)['code'];
                $lines[$key]['is_line'] = current($lines_on_day)['is_line'];
                $lines[$key]['date'] = $key;
            }
        }
        $lines = self::getLineReportMatomo($lines,$date_start,$date_end,'google',$page_abb);
        $lines = self::getLineReportShopSite($lines,$date_start,$date_end,'google',$page_abb);
        return $lines;
         });


        return $lines;
    }
    /**
     * 商品产品线数据报告
     */
    static public function getLineProductReportOld(){
        self::init();
        $lines = DB::table('cc_product_sections')->where('is_line',1)->select('id','admin_id','page_abb','name','code')->get();
        $lines = json_decode(json_encode($lines),true);
        $date_start = post('date_start',date('Y-m-d',strtotime('-7 days')));
        $date_end   = post('date_end',date('Y-m-d'));

        $db_date_end = Carbon::parse($date_end)->addDays(1)->toDateString();
        //获取营销数据
        $params = ['start'=>$date_start,'end'=>$date_end];
        $pts = array_column($lines,'page_abb');
        $pts = implode(',',$pts);

        $params['pts'] = $pts;

        $cacheKey = 'marketing_data.'.md5(json_encode($params));
        $expires = 60;
        
        $marketing_data =  \Cache::tags('product_section_line_v2')->remember($cacheKey,$expires, function () use ($params) {
            $marketing_data = self::getMarketingData('/marketing/pts',$params);
            $marketing_data = array_column($marketing_data['data'] ?? [] ,null,'pt');
            return $marketing_data;
        });

        $sendData = [
            'date_start' => $date_start,
            'date_end'   => $date_end,
            'database_name' => 'scz-' . self::$domain,
            'matomo_site_id' => TraceCode::fetch('matomo_site_id') ?: 66,
            'new_pid_last_days'=>15,
            'all_pid_exposure_qty_low'=>200,
            'new_pid_exposure_qty_low'=>50,
            'new_pid_exposure_qty_upper'=>2000
        ];
        $cacheKey = 'matomo_data.'.md5(json_encode($sendData));

        $matomo_data =  \Cache::tags('product_section_line_v2')->remember($cacheKey,$expires, function () use ($sendData) {
            $matomo_data = self::getLineDataMatomo($sendData,'Project2Handler','getPidLineReport');
            $matomo_data = array_column($matomo_data,null,'section_id');
            return $matomo_data;
        });


        $line_audit_records = DB::table('cc_section_audit_records')->whereBetween('submit_time',[$date_start,$db_date_end])
                            ->select('id','section_id','optimize_type','submit_time')->orderBy('submit_time','asc')->get();
        $marketing_fields = ['product_num','all_num','spend','active_num','unique_visitors','cart_count','new_cart_count','old_cart_count','new_register_count','old_register_count','new_ordered_count','old_ordered_count','new_uc_count','old_uc_count','cost_cart','register_count','cost_register','unique_checkouts','cost_checkout','unique_ordered_count','cost_unique_order','paid_count','paid_total','paid_rate','ga_session','ga_bound','ad_bounce_rate','new_visitor','old_visitor','new_paid_count','old_paid_count','unique_clicks','impressions'];
        $marketing_rates = ['new_cart_proportion','old_cart_proportion','new_order_proportion','old_order_proportion','new_register_proportion','old_register_proportion','new_uc_proportion','old_uc_proportion','marketing_proportion','new_paid_proportion','old_paid_proportion'];
        $marketing_fields = array_merge($marketing_fields,$marketing_rates);

        $lines = array_column($lines,null,'id');
        foreach($lines as $k=>$line){
            $lines[$k]['optimize_sort'] = 0;
            $lines[$k]['optimize_product'] = 0;
            $lines[$k]['optimize_sort_total_interval'] = 0;
            $lines[$k]['optimize_product_total_interval'] = 0;
        }
        foreach($line_audit_records as $record){
            if(!isset($lines[$record->section_id])){
                continue;
            }
            if(!empty($record->optimize_type)){
                
                //只调整了排序
                $lines[$record->section_id]['optimize_product'] += 1;
                if(empty($lines[$record->section_id]['optimize_product_begin_time'])){
                    $lines[$record->section_id]['optimize_product_begin_time'] = $record->submit_time;
                }
                $lines[$record->section_id]['optimize_product_end_time'] = $record->submit_time;
            }

            $lines[$record->section_id]['optimize_sort'] += 1;
            if(empty($lines[$record->section_id]['optimize_sort_begin_time'])){
                $lines[$record->section_id]['optimize_sort_begin_time'] = $record->submit_time;
            }
            $lines[$record->section_id]['optimize_sort_end_time'] = $record->submit_time;
        }

        $exchange_rate = 1;
        foreach($lines as $key=>$line){
            $mk_page_data =  $marketing_data[$line['page_abb']] ?? [];

            foreach($marketing_fields as $field){
                $mk_field = 'mk_'.$field;

                $val = $mk_page_data[$field] ?? 0;
                if(in_array($field,$marketing_rates)){
                    $val = round($val/100,4);
                }

                $lines[$key][$mk_field] = $val;

            }


            $lines[$key]['mk_cost_rate'] = $lines[$key]['mk_marketing_proportion'];

            if($exchange_rate == 1 && !empty($lines[$key]['mk_spend']) && !empty($lines[$key]['mk_paid_total']) && !empty($lines[$key]['mk_cost_rate'])){
                $exchange_rate = round($lines[$key]['mk_cost_rate']*$lines[$key]['mk_paid_total']/$lines[$key]['mk_spend'],2);
            }


            $lines[$key]['mk_paid_rate'] = round($lines[$key]['mk_paid_rate']/100,4);
            $lines[$key]['mk_cart_rate'] = $lines[$key]['mk_unique_visitors'] > 0 ? round($lines[$key]['mk_cart_count']/$lines[$key]['mk_unique_visitors'],4) : 0;
            $lines[$key]['mk_order_rate'] = $lines[$key]['mk_unique_visitors'] > 0 ? round($lines[$key]['mk_unique_ordered_count']/$lines[$key]['mk_unique_visitors'],4) : 0;
            $lines[$key]['mk_register_rate'] = $lines[$key]['mk_unique_visitors'] > 0 ? round($lines[$key]['mk_register_count']/$lines[$key]['mk_unique_visitors'],4) : 0;
            $lines[$key]['mk_checkout_rate'] = $lines[$key]['mk_unique_visitors'] > 0 ? round($lines[$key]['mk_unique_checkouts']/$lines[$key]['mk_unique_visitors'],4) : 0;
            $lines[$key]['mk_ad_bounce_rate'] = round($lines[$key]['mk_ad_bounce_rate']/100,4);
            $lines[$key]['mk_click_rate'] = $lines[$key]['mk_impressions'] > 0 ? round($lines[$key]['mk_unique_clicks']/$lines[$key]['mk_impressions'],4) : 0;
            $lines[$key]['mk_atv'] = $lines[$key]['mk_paid_count'] > 0 ? intval($lines[$key]['mk_paid_total']/$lines[$key]['mk_paid_count']) : 0;

            //产品线优化间隔
            if($line['optimize_sort'] > 1){
                $op_begin = $line['optimize_sort_begin_time'];
                $op_end = $line['optimize_sort_end_time'];
            }else{
                $op_begin = $date_start;
                $op_end   = $db_date_end;
            }
            $time_interval = Carbon::parse($op_begin)->diffInMinutes($op_end);
           
            $divisor = ($line['optimize_sort'] - 1 > 0) ? $line['optimize_sort'] - 1 : $line['optimize_sort'];
            $lines[$key]['optimize_sort_interval'] = $divisor > 0 ? round($time_interval/$divisor/60,2) : 0;

            if(!empty($line['optimize_sort'])){
                $lines[$key]['optimize_sort_total_interval'] = $time_interval;
            }
            

            if($line['optimize_product'] > 1){
                $op_begin = $line['optimize_product_begin_time'];
                $op_end = $line['optimize_product_end_time'];
            }else{
                $op_begin = $date_start;
                $op_end   = $db_date_end;
            }
            $time_interval = Carbon::parse($op_begin)->diffInMinutes($op_end);
            $divisor = ($line['optimize_product'] - 1 > 0) ? $line['optimize_product'] - 1 : $line['optimize_product'];
            $lines[$key]['optimize_product_interval'] = $divisor > 0 ? round($time_interval/$divisor/60,2) : 0;

            if(!empty($line['optimize_product'])){
                $lines[$key]['optimize_product_total_interval'] = $time_interval;
            }

            $page_matomo = $matomo_data[$line['id']] ?? [];
            $lines[$key]['visit_more_1_user_id'] = explode(',',$page_matomo['visit_more_1_user_id'] ?? '');
            $lines[$key]['old_idvisitor_qty'] = $page_matomo['old_idvisitor_qty'] ?? 0;
            $lines[$key]['db_paid_count'] = 0;
            $lines[$key]['visit_more_3_user_id'] = explode(',',$page_matomo['visit_more_3_user_id'] ?? '');
            $lines[$key]['visit_more_3_idvisitor_qty'] =  $page_matomo['visit_more_3_idvisitor_qty'] ?? 0;
            $lines[$key]['db_paid_3_count'] = 0;
            $lines[$key]['visit_more_5_user_id'] = explode(',',$page_matomo['visit_more_5_user_id'] ?? '');
            $lines[$key]['visit_more_5_idvisitor_qty'] = $page_matomo['visit_more_5_idvisitor_qty'] ?? 0;
            $lines[$key]['db_paid_5_count'] = 0;
            $lines[$key]['visit_more_7_user_id'] = explode(',',$page_matomo['visit_more_7_user_id'] ?? '');
            $lines[$key]['visit_more_7_idvisitor_qty'] =  $page_matomo['visit_more_7_idvisitor_qty'] ?? 0;
            $lines[$key]['db_paid_7_count'] = 0;

            $lines[$key]['total_idvisitor_qty'] = $page_matomo['total_idvisitor_qty'] ?? 0;
            $lines[$key]['section_new_pid_qty'] = $page_matomo['section_new_pid_qty'] ?? 0;
            
            $lines[$key]['avg_first_order_time'] = round(($page_matomo['avg_first_order_time'] ?? 0)/60,2);

            $lines[$key]['avg_first_cart_time'] = round(($page_matomo['avg_first_cart_time'] ?? 0)/60,2);
            $lines[$key]['avg_visit_pages'] = $page_matomo['avg_visit_pages'] ?? 0;
            $lines[$key]['avg_visit_total_time'] = round(($page_matomo['avg_visit_total_time'] ?? 0)/60,2);


            $lines[$key]['section_exposure_pid_qty'] = $page_matomo['section_exposure_pid_qty'] ?? 0;
            $lines[$key]['section_total_pid_qty'] = $page_matomo['section_total_pid_qty'] ?? 0;

            $lines[$key]['section_exposure_pid_string'] =  explode(',',$page_matomo['section_exposure_pid_string'] ?? '');
            $lines[$key]['section_new_pid_string'] =  explode(',',$page_matomo['section_new_pid_string'] ?? '');


            $lines[$key]['visit_pages_total'] = (($page_matomo['avg_visit_pages'] ?? 0)*($page_matomo['total_idvisitor_qty'] ?? 0));
            $lines[$key]['first_cart_time_total'] = (($page_matomo['avg_first_cart_time'] ?? 0)*($page_matomo['total_idvisitor_qty'] ?? 0));
            $lines[$key]['first_order_time_total'] = (($page_matomo['avg_first_order_time'] ?? 0)*($page_matomo['total_idvisitor_qty'] ?? 0));
            $lines[$key]['visit_total_time_total'] = (($page_matomo['avg_visit_total_time'] ?? 0)*($page_matomo['total_idvisitor_qty'] ?? 0));            

        }

        //筛选下单数
        $sale_status = OrderStatus::saledStatusIds();
        $db_orders = DB::table('cc_orders')->whereBetween('created_at',[$date_start,$db_date_end])
                    ->whereIn('status_id',$sale_status)
                    ->lists('id','uid');
        
        $db_sales_pids = DB::table('cc_order_products as p')->join('cc_orders as o','o.id','=','p.order_id')
                        ->join('cc_ad_tracks as ad','ad.id','=','o.ad_tracks_id')
                        ->join('cc_order_filters as of','of.order_id','=','o.id')
                        ->where('of.ad_channel','facebook')
                        ->whereBetween('o.created_at',[$date_start,$db_date_end])->whereIn('o.status_id',$sale_status)
                        ->distinct('p.product_id','o.ad_tracks_id')->select('p.product_id','ad.utm_campaign')->get();

        //加购商品
        $add_cart_pids = DB::table('cc_shopping_cart_values as p')
                        ->join('cc_ad_tracks as ad','ad.id','=','p.ad_tracks_id')
                        ->where('ad.utm_source','facebook.com')
                        ->whereBetween('p.created_at',[$date_start,$db_date_end])
                        ->distinct('p.product_id','p.ad_tracks_id')->select('p.product_id','ad.utm_campaign')->get();



        foreach($db_orders as $uid=>$oid){

            foreach($lines as $key=>$line){
                if(in_array($uid,$line['visit_more_1_user_id'])){
                    $lines[$key]['db_paid_count'] += 1;
                    $lines[$key]['order_id'][] = $oid;
                    if(in_array($uid,$line['visit_more_3_user_id'])){
                        $lines[$key]['db_paid_3_count'] += 1;
                    }
                    if(in_array($uid,$line['visit_more_5_user_id'])){
                        $lines[$key]['db_paid_5_count'] += 1;
                    }
                    if(in_array($uid,$line['visit_more_7_user_id'])){
                        $lines[$key]['db_paid_7_count'] += 1;
                    }
                    break;
                }
            }
        }
        $new_pids = DB::table('cc_products')->where('instock_time','>',Carbon::now()->subDays(15)->toDateString())->lists('id');

        foreach($lines as $key=>$line){
            $lines[$key]['db_paid_rate'] = $line['old_idvisitor_qty'] > 0 ? round($line['db_paid_count']/$line['old_idvisitor_qty'],4) : 0;
            $lines[$key]['db_paid_3_count_rate'] = $line['visit_more_3_idvisitor_qty'] > 0 ? round($line['db_paid_3_count']/$line['visit_more_3_idvisitor_qty'],4) : 0;
            $lines[$key]['db_paid_5_count_rate'] = $line['visit_more_5_idvisitor_qty'] > 0 ? round($line['db_paid_5_count']/$line['visit_more_5_idvisitor_qty'],4) : 0;
            $lines[$key]['db_paid_7_count_rate'] = $line['visit_more_7_idvisitor_qty'] > 0 ? round($line['db_paid_7_count']/$line['visit_more_7_idvisitor_qty'],4) : 0;
            unset($lines[$key]['visit_more_1_user_id']);
            unset($lines[$key]['visit_more_3_user_id']);
            unset($lines[$key]['visit_more_5_user_id']);
            unset($lines[$key]['visit_more_7_user_id']);
            $line_pids = DB::table('cc_product_section_selected')->where('section_id',$line['id'])->lists('product_id');
            $lines[$key]['section_total_pid_qty'] = count($line_pids);


            $lines[$key]['section_all_pids_string'] = $line_pids;
            $lines[$key]['section_paid_pids_string'] = [];
            $lines[$key]['db_paid_product_count'] = 0;
            $lines[$key]['db_paid_new_product_count'] = 0;


            
            $lines[$key]['db_new_product_count'] = count(array_intersect($line_pids,$new_pids));
            


            $section_new_pids = $lines[$key]['section_new_pid_string'];
            $section_exposure_pids = $lines[$key]['section_exposure_pid_string'];




            $section_new_pids = array_values(array_intersect($line_pids,$section_new_pids));
            $section_exposure_pids = array_values(array_intersect($line_pids,$section_exposure_pids));

            $lines[$key]['section_exposure_pid_string'] =  $section_exposure_pids;
            $lines[$key]['section_new_pid_string'] =  $section_new_pids;

            $lines[$key]['section_new_pid_qty'] = count($section_new_pids);
            $lines[$key]['section_paid_new_pids_string'] = [];
            $lines[$key]['section_exposure_pid_qty'] = count($section_exposure_pids);

            $lines[$key]['section_add_cart_pids_string'] = [];

            $lines[$key]['section_exposure_pid_rate'] = $lines[$key]['section_total_pid_qty'] > 0 ? round($lines[$key]['section_exposure_pid_qty']/$lines[$key]['section_total_pid_qty'],4) : 0;
        }

        $line_abb_ids = array_column($lines,'id','page_abb');

        //出单商品 使用 -key-匹配
        foreach($db_sales_pids as $key=>$val){


            foreach($lines as $id=>$line){
                if(strpos($val->utm_campaign,'-'.$line['page_abb'].'-')){
                    if(in_array($val->product_id,$line['section_all_pids_string']) && !in_array($val->product_id,$lines[$id]['section_paid_pids_string'])){
                        $lines[$id]['section_paid_pids_string'][] = $val->product_id;
                        $lines[$id]['db_paid_product_count'] += 1;
                        //新品出单款数
                        if(in_array($val->product_id,$line['section_new_pid_string']) && !in_array($val->product_id,$lines[$id]['section_paid_new_pids_string'])){
                            $lines[$id]['section_paid_new_pids_string'][] = $val->product_id;
                            $lines[$id]['db_paid_new_product_count'] += 1;
                        }
                    }
                    break;
                }
            }
        }


        
        $filters = post('filter_conditions',[]);
        $page_abb = post('page_abb','');
        if(!empty($page_abb)){
            $page_abb = explode(',',$page_abb);
        }else{
            $page_abb  = [];
        }

        if(!empty($filters)){
            $lines = collect($lines)->filter(function($item)use($filters,$page_abb){
                if(!empty($page_abb)){
                    if(!in_array($item['page_abb'],$page_abb)){
                        return false;
                    }
                }
                $flag = true;
                foreach($filters as $filter){
                    $min_value = $filter['min_value'];
                    $max_value = $filter['max_value'];
                    $field = $filter['filter_name'];
                    if(!isset($item[$field])){
                        continue;
                    }
                    if(empty($min_value) && empty($max_value)){
                        continue;
                    }
                    if(empty($min_value)){
                        $min_value = 0;
                    }
                    if(empty($max_value)){
                        $max_value = 9999999;
                    }
                    if($item[$field] >= $min_value && $item[$field] <= $max_value){
                        $flag = true;
                    }else{
                        return false;
                    }
                }
                return $flag;
            })->toArray();

        }

        $no_agg_fields = ['id','admin_id','page_abb','name','code','section_exposure_pid_string','section_new_pid_string','section_paid_pids_string','section_paid_new_pids_string','section_all_pids_string','optimize_product_begin_time','optimize_product_end_time','optimize_sort_begin_time','optimize_sort_end_time','section_add_cart_pids_string'];
        $agg = [];
        foreach($lines as $k=>$line){
            foreach($line as $field=>$val){
                if(in_array($field,$no_agg_fields)){
                    continue;
                }
                if(!isset($agg[$field])){
                    $agg[$field] = 0;
                }
                if(is_numeric($line[$field])){
                    $agg[$field] += $line[$field];
                }
                
            }
        }


        $agg['db_paid_rate'] = !empty($agg['old_idvisitor_qty']) ? round($agg['db_paid_count']/$agg['old_idvisitor_qty'],4) : 0;
        $agg['db_paid_3_count_rate'] = !empty($agg['visit_more_3_idvisitor_qty']) ? round($agg['db_paid_3_count']/$agg['visit_more_3_idvisitor_qty'],4) : 0;
        $agg['db_paid_5_count_rate'] = !empty($agg['visit_more_5_idvisitor_qty']) ? round($agg['db_paid_5_count']/$agg['visit_more_5_idvisitor_qty'],4) : 0;
        $agg['db_paid_7_count_rate'] = !empty($agg['visit_more_7_idvisitor_qty']) ? round($agg['db_paid_7_count']/$agg['visit_more_7_idvisitor_qty'],4) : 0;
        $agg['mk_cost_cart'] = !empty($agg['mk_cart_count']) ? round($agg['mk_spend']/$agg['mk_cart_count'],2) : 0;
        $agg['mk_cost_register'] = !empty($agg['mk_register_count']) ? round($agg['mk_spend']/$agg['mk_register_count'],2) : 0;
        $agg['mk_cost_checkout'] = !empty($agg['mk_unique_checkouts']) ? round($agg['mk_spend']/$agg['mk_unique_checkouts'],2) : 0;
        $agg['mk_cost_unique_order'] = !empty($agg['mk_unique_ordered_count']) ? round($agg['mk_spend']/$agg['mk_unique_ordered_count'],2) : 0;
        $agg['mk_paid_rate'] = !empty($agg['mk_unique_ordered_count']) ? round($agg['mk_paid_count']/$agg['mk_unique_ordered_count'],4) : 0;
        $agg['mk_cost_rate'] = !empty($agg['mk_paid_total']) ? round($agg['mk_spend']*$exchange_rate/$agg['mk_paid_total'],4) : 0;
        $agg['mk_cart_rate'] = !empty($agg['mk_unique_visitors']) ? round($agg['mk_cart_count']/$agg['mk_unique_visitors'],4) : 0;
        $agg['mk_order_rate'] = !empty($agg['mk_unique_visitors']) ? round($agg['mk_unique_ordered_count']/$agg['mk_unique_visitors'],4) : 0;
        $agg['mk_register_rate'] = !empty($agg['mk_unique_visitors'] ) ? round($agg['mk_register_count']/$agg['mk_unique_visitors'],4) : 0;
        $agg['mk_checkout_rate'] = !empty($agg['mk_unique_visitors'] ) ? round($agg['mk_unique_checkouts']/$agg['mk_unique_visitors'],4) : 0;

        $agg['mk_new_cart_proportion'] = !empty($agg['mk_new_visitor'] ) ? round($agg['mk_new_cart_count']/$agg['mk_new_visitor'],4) : 0;
        $agg['mk_old_cart_proportion'] =!empty( $agg['mk_old_visitor'] ) ? round($agg['mk_old_cart_count']/$agg['mk_old_visitor'],4) : 0;
        $agg['mk_new_register_proportion'] = !empty($agg['mk_new_visitor'] ) ? round($agg['mk_new_register_count']/$agg['mk_new_visitor'],4) : 0;
        $agg['mk_old_register_proportion'] = !empty($agg['mk_old_visitor'] ) ? round($agg['mk_old_register_count']/$agg['mk_old_visitor'],4) : 0;

        $agg['mk_new_uc_proportion'] = !empty($agg['mk_new_visitor'] ) ? round($agg['mk_new_uc_count']/$agg['mk_new_visitor'],4) : 0;
        $agg['mk_old_uc_proportion'] = !empty($agg['mk_old_visitor'] ) ? round($agg['mk_old_uc_count']/$agg['mk_old_visitor'],4) : 0;
        $agg['mk_new_order_proportion'] = !empty($agg['mk_new_visitor'] ) ? round($agg['mk_new_ordered_count']/$agg['mk_new_visitor'],4) : 0;
        $agg['mk_old_order_proportion'] = !empty($agg['mk_old_visitor'] ) ? round($agg['mk_old_ordered_count']/$agg['mk_old_visitor'],4) : 0;

        $agg['mk_new_paid_proportion'] = !empty($agg['mk_new_visitor'] ) ? round($agg['mk_new_paid_count']/$agg['mk_new_visitor'],4) : 0;
        $agg['mk_old_paid_proportion'] = !empty($agg['mk_old_visitor'] ) ? round($agg['mk_old_paid_count']/$agg['mk_old_visitor'],4) : 0;

        $agg['mk_ad_bounce_rate'] = !empty($agg['mk_ga_session']) ? round($agg['mk_ga_bound']/$agg['mk_ga_session'],4) : 0;

        $agg['section_exposure_pid_rate'] = !empty($agg['section_total_pid_qty']) ? round($agg['section_exposure_pid_qty']/$agg['section_total_pid_qty'],4) : 0;

        $agg['avg_first_cart_time'] = !empty($agg['total_idvisitor_qty']) ? round($agg['first_cart_time_total']/60/$agg['total_idvisitor_qty'],4) : 0;
        $agg['avg_first_order_time'] = !empty($agg['total_idvisitor_qty']) ? round($agg['first_order_time_total']/60/$agg['total_idvisitor_qty'],4) : 0;
        $agg['avg_visit_pages'] = !empty($agg['total_idvisitor_qty']) ? round($agg['visit_pages_total']/$agg['total_idvisitor_qty'],4) : 0;
        $agg['avg_visit_total_time'] = !empty($agg['total_idvisitor_qty']) ? round($agg['visit_total_time_total']/60/$agg['total_idvisitor_qty'],4) : 0;

        $agg['mk_click_rate'] = $agg['mk_impressions'] > 0 ? round($agg['mk_unique_clicks']/$agg['mk_impressions'],4) : 0;
        $agg['mk_atv'] = $agg['mk_paid_count'] > 0 ? intval($agg['mk_paid_total']/$agg['mk_paid_count']) : 0;


        $agg['optimize_sort_interval'] = 0;

        if(!empty($agg['optimize_sort']) && !empty($agg['optimize_sort_total_interval'])){
            $divisor = ($agg['optimize_sort'] - 1 > 0) ? $agg['optimize_sort'] - 1 : $agg['optimize_sort'];
            $agg['optimize_sort_interval'] = $divisor > 0 ? round($agg['optimize_sort_total_interval']/$divisor/60,2) : 0;
        }
        $agg['optimize_product_interval'] = 0;
        if(!empty($agg['optimize_product']) && !empty($agg['optimize_product_total_interval'])){
            $divisor = ($agg['optimize_product'] - 1 > 0) ? $agg['optimize_product'] - 1 : $agg['optimize_product'];
            $agg['optimize_product_interval'] = $divisor > 0 ? round($agg['optimize_product_total_interval']/$divisor/60,2) : 0;
        }
        $s = post('s','total_idvisitor_qty');
        $o = strtolower(post('o','desc'));
        if($o == 'desc'){
            $lines = collect($lines)->sortByDesc($s)->toArray();
        }else{
            $lines = collect($lines)->sortBy($s)->toArray();
        }
        $lines = array_values($lines);

        return ['status'=>200,'data'=>$lines,'agg'=>$agg];
    }

    /**
     * 商品在产品线中的数据表现
     */
    static public function getProductDataInLine(){
        self::init();
        $date_start = post('date_start',date('Y-m-d',strtotime('-7 days')));
        $date_end   = post('date_end',date('Y-m-d'));
        $product_id = intval(post('product_id',0));
        $url_id = post('url_id','');
        if(empty($product_id)){
            return ['status'=>404,'data'=>[]];
        }

        $lines = DB::table('cc_product_section_selected as p')->join('cc_product_sections as s','p.section_id','=','s.id')
                ->where('p.product_id',$product_id)->where('s.is_enabled',1)->where('s.is_line',1)->select('s.*')->get();
        $codes = array_column($lines,'code');
        $lines = array_column($lines,null,'code');

        $newData = $historyData = [];
        if(!empty($codes)){
            $newData = self::getProductDataInLineData($codes, $lines, $date_start, $date_end, $product_id, $url_id);
        }

        //历史信息
        $deletRes = ProductSectionSelectedHistory::getHistoryData($product_id,$date_start,$date_end);
        if(!empty($deletRes['code'])){
            $historyData = $deletRes?self::getProductDataInLineData($deletRes['code'], $deletRes['lines'], $date_start, $date_end, $product_id, $url_id):[];
        }

        $product = Product::select('id','price')->find($product_id);
        $product->f_thumb = $product->getMainThumb('origin')??'';

        return ['status'=>200,  'data'=>['newData'=>$newData, 'historyData' => $historyData, 'product' => $product]];
    }

    /**
     * 获取产品线统计数据
     * @param $codes
     * @param $lines
     * @param $date_start
     * @param $date_end
     * @param $product_id
     * @return array
     */
    private static function getProductDataInLineData($codes, $lines, $date_start, $date_end, $product_id, $url_id = ''){
        $pid_line_url_filter = implode('|',$codes);
        $sendData = [
            'date_start' => $date_start,
            'date_end'   => $date_end,
            'database_name' => 'scz-' . self::$domain,
            'pid_line_url_filter' => $pid_line_url_filter,
            'real_pid'=>$product_id,
        ];
        if($url_id !== ''){
            $sendData['domain_id'] = intval($url_id);
        }
        info('获取产品线统计数据getPidReport/sendData:'.json_encode($sendData));
        $matomo_data = self::getLineDataMatomo($sendData,'Project2ADBHandler','getPidReport');
        info('获取产品线统计数据getPidReport/matomo_data:'.json_encode($matomo_data));

        $domain = '';
        if( $url_id !== '' ){
            if( $url_id == 0 ){
                $url_id = 1;
            }
            $domain = Moresite::where('id',$url_id)->value('domain');
            $domainArr = explode('.', $domain);
            if(count($domainArr) > 2){
                $domainArr = array_slice($domainArr, -2);// 倒数2个
            }
            $domain = rtrim(implode('.', $domainArr), "/");
        }

        $sale_status = OrderStatus::saledStatusIds();
        $date_start = Carbon::parse($date_start . ' 00:00:00')->addHour()->toDateTimeString();
        $date_end = Carbon::parse($date_end . ' 23:59:59')->addHour()->toDateTimeString();

        $db = DB::table('cc_order_products as p')
            ->join('cc_orders as o','o.id','=','p.order_id')
            ->join('cc_ad_tracks as ad','ad.id','=','o.ad_tracks_id')
            ->whereBetween('o.created_at',[$date_start,$date_end])
            ->whereIn('o.status_id',$sale_status)
            ->where('p.product_id',$product_id)
            ->groupBy('o.email')
            ->select(
                'p.product_id',
                'o.id',
                'o.email',
                'ad.utm_campaign',
                'ad.ad_url',
                'p.qty'
            );
        $domain && $db = $db->where('o.domain','like','%'.$domain.'%');
        $sales = $db->get();
        if(get('debug_info') == 1) {
            info('获取产品sales数据:' . json_encode($sales));
            info('获取产品lines数据:' . json_encode($lines));
        }
        
        $lineSales = [];
        foreach($sales as $key=>$val){
            foreach($lines as $line){
                $page_abb = $line->page_abb;
                if(empty($page_abb)){
                    continue;
                }
                $line_code = $line->code;
                $utm_campaign = explode('-',$val->utm_campaign);
                $k = count($utm_campaign) - 4;
                if(strpos($val->ad_url,$line_code) !== false || (isset($utm_campaign[$k]) && $utm_campaign[$k] == $page_abb)){
                    if(!isset($lineSales[$page_abb])){
                        $lineSales[$page_abb] = 0;
                    }
                    $lineSales[$page_abb] += 1;
                    break;
                }
            }

        }

        $db = DB::table('cc_order_products as p')
            ->join('cc_orders as o','o.id','=','p.order_id')
            ->join('cc_ad_tracks as ad','ad.id','=','o.ad_tracks_id')
            ->whereBetween('o.created_at',[$date_start,$date_end])
            ->whereIn('o.status_id',$sale_status)
            ->where('p.product_id',$product_id)
            ->select(
                'p.product_id',
                'o.id',
                'ad.ad_url',
                'ad.utm_campaign',
                'p.qty'
            );
        $domain && $db = $db->where('o.domain','like','%'.$domain.'%');
        $orderProduct = $db->get();
        $orderProductTotal = [];
        foreach($orderProduct as $key=>$val){
            foreach($lines as $line){
                $page_abb = $line->page_abb;
                if(empty($page_abb)){
                    continue;
                }
                $line_code = $line->code;
                $utm_campaign = explode('-',$val->utm_campaign);
                $k = count($utm_campaign) - 4;
                if(strpos($val->ad_url,$line_code) !== false || (isset($utm_campaign[$k]) && $utm_campaign[$k] == $page_abb)){
                    if(!isset($orderProductTotal[$page_abb])){
                        $orderProductTotal[$page_abb] = 0;
                    }
                    $orderProductTotal[$page_abb] += $val->qty ?? 0;
                    break;
                }
            }
        }

        $pts = array_column($lines,'page_abb');
        $pts = implode(',',$pts);
        $params = ['start'=>$sendData['date_start'],'end'=>$sendData['date_end'],'pts'=>$pts];

        //广告数据需要的字段
        $marketing_fields = ['all_num','active_num','paused_num','spend','active_speed','paused_speed','cost_per_unique_click','ctr','ad_bounce_rate','cart_proportion','cart_count','cost_cart'];
        $reserveArr = ['ctr','ad_bounce_rate','cart_proportion'];
        $marketing_data = self::getMarketingData('/marketing/pts',$params);
        $marketing_agg = $marketing_data['agg'] ?? [];
        $marketing_data = array_column($marketing_data['data'] ?? [] ,null,'pt');

        $agg = ['checkout_qty'=>0,'cart_uv'=>0,'cart_pv'=>0,'exposure_pv'=>0,'checkout_uv'=>0,'exposure_uv'=>0,'click_uv'=>0,'click_pv'=>0,'sale_qty'=>0,'order_product_total'=>0];

        $intval_fields  = array_keys($agg);
        $rate_fields  = ['checkout_rate_uv','click_rate_uv','click_rate_pv','cart_rate_uv'];
        foreach($matomo_data as $key=>$val){
            $matomo_data[$key]['page_abb'] = $lines[$val['entry_url']]->page_abb ?? self::getPageAbbByCode($val['entry_url']);
            $saleQty = $lineSales[$matomo_data[$key]['page_abb']] ?? self::getSalesVolume($product_id,$val['entry_url'],$matomo_data[$key]['page_abb'],$date_start,$date_end,$sale_status,$domain,1);
            if(get('debug_info') == 1) {
                info('获取产品线saleQty数据:'.$matomo_data[$key]['page_abb'].'-'. $saleQty);
            }
            $matomo_data[$key]['entry_url'] = $val['entry_url'] ?? '';
            $matomo_data[$key]['sale_qty'] = $saleQty;
            $matomo_data[$key]['exposure_conversion'] = $saleQty > 0 ? intval($matomo_data[$key]['exposure_uv']/$saleQty) : 0;
            $matomo_data[$key]['paid_rate_uv'] = $matomo_data[$key]['click_uv'] > 0 ? round($matomo_data[$key]['sale_qty']/$matomo_data[$key]['click_uv'],4) : 0;
            $matomo_data[$key]['order_product_total'] = $orderProductTotal[$matomo_data[$key]['page_abb']] ?? self::getSalesVolume($product_id,$val['entry_url'],$matomo_data[$key]['page_abb'],$date_start,$date_end,$sale_status,$domain);
            foreach($intval_fields as $field){
                $matomo_data[$key][$field] = intval($matomo_data[$key][$field]);
                $agg[$field] += $matomo_data[$key][$field];
            }

            //广告数据赋值
            $mk_page_data = $marketing_data[$matomo_data[$key]['page_abb']] ?? [];
            foreach($marketing_fields as $field){
                $mk_field = 'market_'.$field;
                $mk_val = $mk_page_data[$field] ?? 0;
                if($field == 'atv'){
                    $mk_val = intval($mk_val);
                }
                $matomo_data[$key][$mk_field] = in_array($field,$reserveArr) ? bcdiv($mk_val,100,4) : $mk_val;
            }

            foreach($rate_fields as $field){
                $matomo_data[$key][$field] = round($val[$field],4);
            }
        }
        $agg['checkout_rate_uv'] = $agg['click_uv'] > 0 ? round($agg['checkout_uv']/$agg['click_uv'],4) : 0;
        $agg['click_rate_uv'] = $agg['exposure_uv'] > 0 ? round($agg['click_uv']/$agg['exposure_uv'],4) : 0;
        $agg['click_rate_pv'] = $agg['exposure_pv'] > 0 ? round($agg['click_pv']/$agg['exposure_pv'],4) : 0;
        $agg['cart_rate_uv'] = $agg['click_uv'] > 0 ? round($agg['cart_uv']/$agg['click_uv'],4) : 0;
        $agg['paid_rate_uv'] = $agg['click_uv'] > 0 ? round($agg['sale_qty']/$agg['click_uv'],4) : 0;
        $agg['exposure_conversion'] = $agg['sale_qty'] > 0 ? intval($agg['exposure_uv']/$agg['sale_qty']) : 0;

        foreach($marketing_fields as $field){
            $mk_field = 'market_'.$field;
            $mk_val = $marketing_agg[$field] ?? 0;
            if($field == 'atv'){
                $mk_val = intval($mk_val);
            }
            $agg[$mk_field] = in_array($field,$reserveArr) ? bcdiv($mk_val,100,4) : $mk_val;
        }

        return ['matomo_data'=>$matomo_data,'agg'=>$agg];
        //return ['status'=>200,'data'=>$matomo_data,'agg'=>$agg];
    }

    public static function getPageAbbByCode($code)
    {
        $pageAbb = DB::table('cc_product_sections')->where('code',$code)->value('page_abb');
        return $pageAbb ?? '' ;
    }

    /**
     * 获取不存在勾选位产品的销量
     * @param $product_id
     * @param $code
     * @param $page_abb
     * @param $date_start
     * @param $date_end
     * @param $sale_status
     * @param $domain
     * @param int $type
     * @return int
     */
    public static function getSalesVolume($product_id,$code,$page_abb,$date_start,$date_end,$sale_status,$domain,$type=0)
    {
        $sql = '(position( ? in ad.utm_campaign) > 0 or position( ? in ad.ad_url) > 0)';
        $db = DB::table('cc_order_products as op')
            ->join('cc_orders as o','o.id','=','op.order_id')
            ->join('cc_ad_tracks as ad','ad.id','=','o.ad_tracks_id')
            ->whereBetween('o.created_at',[$date_start,$date_end])
            ->whereIn('o.status_id',$sale_status)
            ->whereRaw($sql,[$page_abb,$code])
            ->where('op.product_id',$product_id);
        $domain && $db = $db->where('o.domain','like','%'.$domain.'%');
        if( $type ){
            $data = $db->groupBy('o.email')->select('op.id')->get();
            return  $data ? count($data) : 0;
        }
        return $db->sum('op.qty') ?: 0;
    }

    /**
     * 产品线商品数据报告
     */
    static public function getLineProductMembersReport(){
        $section_id = intval(post('section_id',0));
        $section = DB::table('cc_product_sections')->where('id',$section_id)->first();
        $size = intval(post('size',60));
        $page = intval(post('page',1));
        if(empty($section)){
            return ['status'=>301,'msg'=>'产品线不存在'];
        }
        $date_start = post('date_start',date('Y-m-d',strtotime('-7 days')));
        $date_end   = post('date_end',date('Y-m-d'));

        $before_date_start = post('before_date_start','');
        $before_date_end = post('before_date_end','');

        $effective_status = post('effective_status','ALL');
        $admin_users = UserService::getAdminUserNames();
        $sortField = post('sort','');
        $sortType = post('order','desc');
        $search_pid = intval(post('search_pid',0));
        $admin_id = intval(post('admin_id',0));
        $pids_query = DB::table('cc_product_section_selected as s')->where('s.section_id',$section_id);
        if(!empty($search_pid)){
            $pids_query->where('s.product_id',$search_pid);
        }
        if(!empty($admin_id)){
            $pids_query->join('cc_products as p','s.product_id','=','p.id')->where('p.admin_id',$admin_id);
        }
        $pids = $pids_query->lists('s.product_id');
        if(empty($pids)){
            return ['status'=>200,'data'=>[],'agg'=>[],'total'=>0];
        }

        $line_pids = $pids = ProductService::filterProductIdByStatus($pids,['instock']);
        $site_id = 0;
        $json_data = [
                'date_start'    =>$date_start,
                'date_end'      =>$date_end,
                'page'          =>$page,
                'size'          =>$size,
                'site_id'       =>$site_id,
                'product_ids'   => $pids
        ];
        $effective_status = $effective_status == 'NONE' ? '' : $effective_status;
        if($effective_status != 'ALL'){
            $json_data['effective_status'] = $effective_status;
        }

        if(!empty($sortField)){
            $json_data['order_field'] = $sortField;
            $json_data['order_type'] = $sortType;
        }
        $go_data = self::getProductsDataFromGo('/stats/products',$json_data);
        $data = [];
        $agg =  [];
        if(!empty($go_data['data'])){
            $data = $go_data['data']['list'] ?? [];
            unset($go_data['data']['list']);
            $agg = $go_data['data'];
        }
        $before_data = [];
        $before_agg = [];
        if(!empty($data)){
            $pids = array_column($data,'product_id');
            $products = ProductService::getProductsByIdsFromCache($pids);
            $products = array_column($products,null,'id');
            $db_data = DB::table('cc_products')->whereIn('id',$pids)->select('id','admin_id','spu','updated_at','instock_time')->get();
            $db_data = array_column($db_data,null,'id');
            $before_go_data = [];
            if(!empty($before_date_start) && !empty($before_date_end)){
                $json_data = [
                    'date_start'    =>$before_date_start,
                    'date_end'      =>$before_date_end,
                    'page'          =>1,
                    'size'          =>count($line_pids),
                    'site_id'       =>$site_id,
                    'product_ids'   => $line_pids
                ];
                $go_response = self::getProductsDataFromGo('/stats/products',$json_data);
                if(!empty($go_response['data']['list'])){
                    $before_go_data = array_column($go_response['data']['list'],null,'product_id');
                    unset($go_response['data']['list']);
                    $before_agg = $go_response['data'];
                }
                
            }

            $before_exp = $data[0] ?? [];
            foreach($before_exp as $k=>$v){
                $before_exp[$k] = 0;
            }
            foreach($data as $k=>$v){

                $pid = $v['product_id'];

                if(!empty($before_go_data)){
                    $before_exp['product_id'] = $pid;
                    $before_data[]= $before_go_data[$pid] ?? $before_exp;
                }


                $admin_id = $db_data[$pid]->admin_id ?? 0;
                $data[$k]['f_thumb'] = $products[$pid]['f_thumb'] ?? '';
                $data[$k]['price']   = $products[$pid]['price'] ?? 0;
                $data[$k]['admin_user'] = $admin_users[$admin_id]['name'] ?? '--';
                $data[$k]['spu']        = $db_data[$pid]->spu ?? '';
                $data[$k]['updated_at'] = !empty($db_data[$pid]->updated_at) ? Carbon::parse($db_data[$pid]->updated_at)->toDateString() : '';
                $data[$k]['instock_time'] = !empty($db_data[$pid]->instock_time) ? Carbon::parse($db_data[$pid]->instock_time)->toDateString() : '';
            }
        }
        return ['status' => 200,'total'=>$agg['total'] ?? 0 ,'data'=>$data,'agg'=>$agg,'before_data'=>$before_data,'before_agg'=>$before_agg];
    }
    /**
     * 添加产品线操作记录
     */
    static public function insertLineOperateLog($section_id,$field,$new_value,$old_value,$note = ''){
        $backendUser = \BackendAuth::getUser();
        $now = date('Y-m-d H:i:s');
        $log = [
            'field' => $field,
            'relation_type' => "ProductSectionLine",
            'relation_id' => $section_id,
            'user_id' => $backendUser->id ?? 1,
            'created_at' => $now,
            'updated_at' => $now,
            'old_value' => $old_value,
            'new_value' => $new_value,
            'note' => $note
        ];
        DB::table('cc_admin_operation_logs')->insert($log);
    }
    /** 
     * 获取产品线操作记录
     */
    static public function getLineOperateLogs(){
        $section_id = post('section_id',0);
        $size = intval(post('size',20));
        $items = DB::table('cc_admin_operation_logs')->where('relation_id',$section_id)
                ->where('relation_type','ProductSectionLine')
                ->orderBy('id','desc')->paginate($size)->toArray();
        $admin_users = UserService::getAdminUserNames();
        $fields = ['name'=>'名称','code'=>'CODE','page_abb'=>'缩写'];
        foreach($items['data'] as $k=>$v){
            $v->admin_user = $admin_users[$v->user_id]['name'] ?? '';
            $v->result = '成功';
            $v->msg = '';
            if(in_array($v->field,['id'])){
                $v->msg = '创建产品线';
            }elseif($v->field == 'audit_status' && $v->new_value == 1){
                $v->msg = '提交审核';
            }elseif($v->field == 'audit_status' && in_array($v->new_value,[2,3])){
                $v->result = $v->new_value == 3 ? '驳回' : '成功';
                $v->msg = '返回审核结果';
            }elseif(in_array($v->field,['name','code','page_abb'])){
                $v->msg = '更改'.$fields[$v->field].'为'.$v->new_value;
            }elseif($v->field == 'admin_id'){
                $v->msg = '更改所属用户为'.($admin_users[$v->new_value]['name'] ?? '');
            }
        }
        $items['status'] = 200;
        return $items;

    }
    /**
     * 商品产品线数据报表标签匹配
     */
    static public function searchLineProductTags(){
        $tags = post('tags','');

        $section_ids = DB::table('cc_product_sections')->where('is_enabled',1)->where('is_line',1)->lists('id');
        $data = [];
        foreach($section_ids as $id){
            $data[$id] = 0;
        }
        if(empty($tags)){
            return ['status'=>200,'data'=>$data];
        }
        $tags = explode(',',$tags);
        $pids = [];
        
        $tag_pids = DB::table('cc_products_to_tags as p')->join('cc_tags as t','p.tag_id','=','t.id')
                    ->join('cc_product_section_selected as s','s.product_id','=','p.product_id')
                    ->whereIn('s.section_id',$section_ids)
                    ->whereIn('t.name_cn',$tags)
                    ->select('p.product_id','s.section_id')
                    ->get();
        $start = time();
        foreach($tag_pids as $k=>$v){
            if(isset($data[$v->section_id])){
                $data[$v->section_id] += 1;
            }
        }
        return ['status'=>200,'data'=>$data];
    }

    /**
     * 销量分布-整站数据
     */
    static public function getSalesDistributionSite(){
        $rang = [1=>'1',5=>'2~5',10=>'6~10',30=>'11~30',50=>'31~50',100=>'51~100'];

        $items =self::getProductSalesData();
        $total = count($items);
        $data = [];
        foreach($rang as $v){
            $data[$v]['sales'] = 0;
            $data[$v]['pids'] = [];
        }
        $data['100以上']['sales'] = 0;
        $data['100以上']['pids'] = [];
        if(!empty($total)){
            foreach($items as $pid=>$val){
                foreach($rang as $k=>$v){
                    if($val <= $k){
                        $data[$v]['sales'] += 1;
                        $data[$v]['pids'][]=$pid;
                        continue 2;
                    }
                }
                $data['100以上']['sales'] += 1;
                $data['100以上']['pids'][]=$pid;
            }
        }

        $result = [];
        foreach($data as $key=>$val){
            $result[] = ['title'=>$key,'count'=>$val['sales'],'rate'=>$total ? round($val['sales']/$total,4) : 0,'pids'=>$val['pids']];
        }
        $agg = ['title'=>'合计','count'=>$total,'rate'=>$total ? 1 : 0];
        return ['status'=>200,'data'=>$result,'agg'=>$agg];
    }
    /**
     * 销量分布
     */
    static public function getProductSalesData(){
        //下单时间
        $date_start = post('date_start',date('Y-m-d',strtotime('-7 days')));
        $date_end   = post('date_end',date('Y-m-d'));
        $date_end = Carbon::parse($date_end)->addDays(1)->toDateString();

        //在库时间
        $instock_time_start = post('instock_time_start',date('Y-m-d',strtotime('-7000 days')));
        $instock_time_end   = post('instock_time_end',date('Y-m-d'));
        $instock_time_end = Carbon::parse($instock_time_end)->addDays(1)->toDateString();
        $sale_status = OrderStatus::saledStatusIds();
        $items = DB::table('cc_order_products as op')
                ->join('cc_orders as o','o.id','=','op.order_id')
                ->join('cc_products as p','p.id','=','op.product_id')
                ->whereBetween('o.created_at',[$date_start,$date_end])
                ->whereBetween('p.instock_time',[$instock_time_start,$instock_time_end])
                ->whereIn('o.status_id',$sale_status)
                ->groupBy('op.product_id')
                ->select(DB::raw('sum(op.qty) as count'),'op.product_id as pid')
                ->lists('count','pid');
        return $items;
    }
    /**
     * 销量分布-产品线数据
     */
    static public function getSalesDistributionLine(){
        $items = self::getProductSalesData();
        $lines = DB::table('cc_product_sections')->where('is_line',1)->select('id','page_abb','name')->get();
        $rang = [1=>'1',5=>'2~5',10=>'6~10',30=>'11~30',50=>'31~50',100=>'51~100'];

        $agg = ['product_count'=>0,'sales_total'=>0];
        $agg_total = 0;
        foreach($lines as $key=>$line){
            $sale_range = [];
            foreach($rang as $v){
                $sale_range[$v] = 0;
            }
            $sale_range['100+'] = 0;
            $pids = DB::table('cc_product_section_selected')->where('section_id',$line->id)->lists('product_id');
            $lines[$key]->product_count = count($pids);
            $lines[$key]->sales_total = 0;

            $total = 0;
            foreach($items as $pid=>$qty){
                if(in_array($pid,$pids)){
                    $total += 1;
                    $lines[$key]->sales_total += $qty;
                    foreach($rang as $k=>$v){
                        if($qty <= $k){
                            $sale_range[$v] += 1;
                            continue 2;
                        }
                    }
                    $sale_range['100+'] += 1;
                }
            }
            $agg_total += $total;
            $agg['product_count'] += $lines[$key]->product_count;
            $agg['sales_total'] += $lines[$key]->sales_total;
            foreach($sale_range as $index=>$count){
                $lines[$key]->$index = $count;
                $rate_index = $index.'_rate';
                $lines[$key]->$rate_index = $total ? round($count/$total,4) : 0;
                if(!isset($agg[$index])){
                    $agg[$index] = 0; 
                }
                $agg[$index] += $count;
            }
            $lines[$key]->pin_rate = $lines[$key]->product_count ?  round($total/$lines[$key]->product_count,4) : 0;

        }
        foreach($rang as $v){
            $agg[$v.'_rate'] = $agg_total ? round($agg[$v]/$agg_total,4) : 0;
        }
        $agg['100+_rate'] = $agg_total ? round($agg['100+']/$agg_total,4) : 0;
        $agg['pin_rate']  = $agg['product_count'] ? round($agg_total/$agg['product_count'],4) : 0;
        return ['status'=>200,'data'=>$lines,'agg'=>$agg];

    }
    /**
     * 销量分布-广告销量数据
     */
    static public function getSalesDistributionAd(){
         //下单时间
        $date_start = post('date_start',date('Y-m-d',strtotime('-7 days')));
        $date_end   = post('date_end',date('Y-m-d'));
        $date_end = Carbon::parse($date_end)->addDays(1)->toDateString();
        $filter_campaigns = post('filter_campaigns',[]);

        //在库时间
        $instock_time_start = post('instock_time_start',date('Y-m-d',strtotime('-300 days')));
        $instock_time_end   = post('instock_time_end',date('Y-m-d'));
        $instock_time_end = Carbon::parse($instock_time_end)->addDays(1)->toDateString();
        $sale_status = OrderStatus::saledStatusIds();
        $items = DB::table('cc_order_products as op')
                ->join('cc_orders as o','o.id','=','op.order_id')
                ->join('cc_products as p','p.id','=','op.product_id')
                ->whereBetween('o.created_at',[$date_start,$date_end])
                ->whereBetween('p.instock_time',[$instock_time_start,$instock_time_end])
                ->whereIn('o.status_id',$sale_status)
                ->groupBy('op.product_id')
                ->groupBy('o.ad_tracks_id')
                ->select(DB::raw('sum(op.qty) as count'),'op.product_id as pid','o.ad_tracks_id as ad_tracks_id')
                ->get();
        $ad_tracks_ids =array_values(array_unique(array_column($items,'ad_tracks_id')));
        $ad_tracks = DB::table('cc_ad_tracks')->whereIn('id',$ad_tracks_ids)->orderBy('id','asc')->get();
        $ad_tracks = json_decode(json_encode($ad_tracks),true);

        $ad_campaigns = [];
        $rang = [1=>'1',5=>'2~5',10=>'6~10',30=>'11~30',50=>'31~50',100=>'51~100'];
        foreach($ad_tracks as $ad_track){
            if(!empty($filter_campaigns) && !in_array($ad_track['utm_campaign'],$filter_campaigns)){
                continue;
            }
            $label = \Jason\Ccshop\Controllers\AdTracks::divideChannels($ad_track);
            if(in_array($label,['facebook','facebook.com'])){
                $ad_campaigns[$ad_track['id']]['utm_campaign'] = $ad_track['utm_campaign'];
                foreach($rang as $v){
                    $ad_campaigns[$ad_track['id']][$v] = 0;
                }
                $ad_campaigns[$ad_track['id']]['100+'] = 0;
                $ad_campaigns[$ad_track['id']]['sales_product'] = 0;
                $ad_campaigns[$ad_track['id']]['sales_total'] = 0;
            }
        }
        foreach($items as $index=>$item){
            if(!isset($ad_campaigns[$item->ad_tracks_id])){
                continue;
            }
            $ad_campaigns[$item->ad_tracks_id]['sales_product'] += 1;
            $ad_campaigns[$item->ad_tracks_id]['sales_total'] += $item->count;

            foreach($rang as $k=>$v){

                if($item->count <= $k){
                    $ad_campaigns[$item->ad_tracks_id][$v] += 1;
                    continue 2;
                }
            }
            $ad_campaigns[$item->ad_tracks_id]['100+'] += 1;
        }
        $result = [];
        $codes = [];

        foreach($ad_campaigns as $val){
            if(!isset($result[$val['utm_campaign']])){
                $result[$val['utm_campaign']] = $val;
            }else{
                foreach($rang as $v){
                    $result[$val['utm_campaign']][$v] += $val[$v];
                }
                $result[$val['utm_campaign']]['100+'] += $val['100+'];
                $result[$val['utm_campaign']]['sales_product'] += $val['sales_product'];
                $result[$val['utm_campaign']]['sales_total'] += $val['sales_total'];
            }
            $arr = explode('-',$val['utm_campaign']);
            $code = strtoupper($arr[count($arr) - 4] ?? '');
            if(!in_array($code,$codes)){
                $codes[] = $code;
            }
            $result[$val['utm_campaign']]['code'] = $code;
        }
        //产品线
        $lines = DB::table('cc_product_sections')->whereIn('page_abb',$codes)->lists('id','page_abb');
        $lines_product = DB::table('cc_product_section_selected')->whereIn('section_id',array_values($lines))
                        ->groupBy('section_id')->select(DB::raw('count(product_id) as count'),'section_id')->lists('count','section_id');
        $agg = ['sales_total'=>0,'product_count'=>0,'sales_product'=>0];
        foreach($result as $key=>$val){
            foreach($rang as $v){
                if(!isset($agg[$v])){
                    $agg[$v] = 0;
                }
                $agg[$v] += $val[$v];


                $result[$key][$v.'_rate'] = $result[$key]['sales_product'] ? round($result[$key][$v]/$result[$key]['sales_product'],4) : 0;
            }
            if(!isset($agg['100+'])){
                $agg['100+'] = 0;
            }
            $agg['100+'] += $val['100+'];
            $result[$key]['100+_rate'] = $result[$key]['sales_product'] ? round($result[$key]['100+']/$result[$key]['sales_product'],4) : 0;
            $result[$key]['product_count'] = $lines_product[$lines[$result[$key]['code']] ?? 0] ?? 0;
            $result[$key]['pin_rate'] = $result[$key]['product_count'] ? round($result[$key]['sales_product']/$result[$key]['product_count'],4) : 0;
            $agg['sales_total'] += $val['sales_total'];
            $agg['product_count'] += $result[$key]['product_count'];
            $agg['sales_product'] += $result[$key]['sales_product'];
        }

        foreach($rang as $v){
            $agg[$v.'_rate'] = $agg['sales_product'] ? round($agg[$v]/$agg['sales_product'],4) : 0;
        }
        $agg['100+_rate'] = $agg['sales_product'] ? round($agg['100+']/$agg['sales_product'],4) : 0;
        $agg['pin_rate'] = $agg['product_count'] ? round($agg['sales_product']/$agg['product_count'],4) : 0;

        $filter_result = [];
        if(!empty($filter_campaigns)){
            foreach($result as $k=>$v){
                if(in_array($k,$filter_campaigns)){
                    $filter_result[$k] = $v;
                }
            }
            return ['status'=>200,'filter_data'=>$filter_result];
        }

        $size = intval(post('size',50));
        $result = collect(array_values($result))->sortByDesc('sales_total')->toArray();
        $result = Helper::manualPaging($result,['perPage'=>$size]);
        $result['status'] = 200;
        $result['agg'] = $agg;
        return $result;
    }
    /**
     * 销量分布-主页数据
     */
    static public function getSalesDistribution(){
        $date_start = post('up_begin_time',date('Y-m-d',strtotime('-7 days')));
        $date_end   = post('up_end_time',date('Y-m-d'));
        $diff_days = Carbon::parse($date_end)->diffInDays($date_start,true);
        $date_end = Carbon::parse($date_end)->addDays(1)->toDateString();
        
        $before_date_end = post('down_end_time',$date_start);
        $before_date_start = post('down_begin_time',Carbon::parse($before_date_end)->subDays($diff_days)->toDateString());

        $before_date_end = Carbon::parse($before_date_end)->addDays(1)->toDateString();
        
        $product_totals = DB::table('cc_products')
                        ->where('status','instock')->where('instock_time','<',$date_end)
                        ->count('id');
        $before_product_totals = DB::table('cc_products')
                        ->where('status','instock')->where('instock_time','<',$before_date_end)
                        ->count('id');
        $sale_status = OrderStatus::saledStatusIds();
        $sp_query = DB::table('cc_order_products as op')
                ->join('cc_orders as o','o.id','=','op.order_id')
                ->whereIn('o.status_id',$sale_status)
                ->distinct('op.product_id');
        $before_sp_query = clone($sp_query);
        $sales_count    = $sp_query->whereBetween('o.created_at',[$date_start,$date_end])->count('op.product_id');
        $before_sales_count = $before_sp_query->whereBetween('o.created_at',[$before_date_start,$before_date_end])->count('op.product_id');

        $sc_query = DB::table('cc_order_products as op')
                ->join('cc_orders as o','o.id','=','op.order_id')
                ->whereIn('o.status_id',$sale_status);
        $before_sc_query = clone($sc_query);

        $sales_total = $sc_query->whereBetween('o.created_at',[$date_start,$date_end])->sum('qty');
        $before_sales_total = $before_sc_query->whereBetween('o.created_at',[$before_date_start,$before_date_end])->sum('qty');

        $up_data = ['sales_total'=>intval($sales_total),'pin_rate'=>$product_totals ? round(intval($sales_count)/$product_totals,4) : 0];
        $down_data = ['sales_total'=>intval($before_sales_total),'pin_rate'=>$product_totals ? round(intval($before_sales_count)/$before_product_totals,4) : 0];


        return ['status'=>200,'up_data'=>$up_data,'down_data'=>$down_data];
        
    }

    static public function getOperateIndexDataSales(){
        $date_start = post('date_start',date('Y-m-d'));
        $date_end = post('date_end',date('Y-m-d'));
        $date_end = Carbon::parse($date_end)->addDays(1)->toDateString();
        $sale_status = OrderStatus::saledStatusIds();
        $sales_data = DB::table('cc_order_products as op')
                    ->join('cc_orders as o','o.id','=','op.order_id')
                    ->join('cc_products as p','p.id','=','op.product_id')
                    ->whereBetween('o.created_at',[$date_start,$date_end])
                    ->whereIn('o.status_id',$sale_status)
                    ->groupBy('p.admin_id')
                    ->groupBy('op.product_id')
                    ->select('p.admin_id','op.product_id',DB::raw('sum(op.qty) as qty'))
                    ->get();
        $lines_product = DB::table('cc_product_section_selected as p')
                    ->join('cc_product_sections as s','s.id','=','p.section_id')
                    ->where('s.is_line',1)
                    ->where('s.admin_id','>',0)
                    ->groupBy('s.admin_id')->groupBy('p.product_id')
                    ->select('s.admin_id','p.product_id')
                    ->get();
        $lines_data  = [];

        $rank_fields = ['sales_total','sales_count','wishs_count','line_product_count','line_sales_count'];
        $rank_data = [];
        foreach($lines_product as $k=>$v){
            $lines_data[$v->product_id][]=$v->admin_id;
            if(!isset($rank_data[$v->admin_id])){
                $rank_data[$v->admin_id] = [
                    'admin_id' =>$v->admin_id,
                    'sales_total' => 0,
                    'sales_count' => 0,
                    'wishs_count' => 0,
                    'line_product_count' => 0,
                    'line_sales_count' => 0
                ];
            }
            $rank_data[$v->admin_id]['line_product_count'] += 1;
        }
        foreach($sales_data as $k=>$v){
            if(!isset($rank_data[$v->admin_id])){
                $rank_data[$v->admin_id] = [
                    'admin_id' =>$v->admin_id,
                    'sales_total' => 0,
                    'sales_count' => 0,
                    'wishs_count' => 0,
                    'line_product_count'=>0,
                    'line_sales_count' => 0
                ];
            }
            $rank_data[$v->admin_id]['sales_total'] += $v->qty;
            $rank_data[$v->admin_id]['sales_count'] += 1;
            //产品线销量
            if(isset($lines_data[$v->product_id])){
                $line_admins = $lines_data[$v->product_id];
                foreach($line_admins as $admin_id){
                    $rank_data[$admin_id]['line_sales_count'] += 1;
                }
            }
        }
        $wishs_data = DB::table('cc_wishlists as w')
                    ->join('cc_products as p','p.id','=','w.product_id')
                    ->whereBetween('w.created_at',[$date_start,$date_end])
                    ->groupBy('p.admin_id')
                    ->groupBy('w.product_id')
                    ->select('p.admin_id','w.product_id')
                    ->get();
        foreach($wishs_data as $v){
            if(!isset($rank_data[$v->admin_id])){
                $rank_data[$v->admin_id] = [
                    'admin_id' =>$v->admin_id,
                    'sales_total' => 0,
                    'sales_count' => 0,
                    'wishs_count' => 0,
                    'line_product_count'=>0,
                    'line_sales_count' => 0
                ];
            }
            $rank_data[$v->admin_id]['wishs_count'] += 1;
        }


        $result = [];

        $groupData = self::dataChangeGroup($rank_data,$rank_fields);
        $rank_data = $groupData['rankData'];
        $curIndex = $groupData['curIndex'];
        foreach($rank_fields as $field){
            $result[$field] = self::getRankDataByField($rank_data,$field,$curIndex);
        }
        return ['status'=>200,'data'=>$result];
    }

    static public function getRankDataByField($data,$field,$admin_id){

        $data = collect($data)->sortByDesc($field)->toArray();
        $data = array_values($data);
        $rank = 0;
        $step = 1;
        $first = 0;
        $front = 0;

        $user_data = [$field=>0,'rank'=>0,'diff_first'=>0,'diff_front'=>0];

        if(empty($data)){
            return $user_data;
        }
        foreach($data as $k=>$val){
            if(isset($data[$k-1]) && $data[$k-1][$field] == $val[$field]){
                $step += 1;   
            }else{
                $rank += $step;
                $step = 1;
            }
            $data[$k]['rank'] = $rank;
            if($val['admin_id'] == $admin_id){
                $user_data = [$field=>$val[$field],'rank'=>$rank];
            }

        }

        $first = $data[0][$field];

        foreach($data as $k=>$val){
            if($val['rank'] < $user_data['rank']){
                $front = $val[$field];
            }
        }
        $user_data['diff_first'] = $first - $user_data[$field];
        $user_data['diff_front'] = $front ? $front - $user_data[$field] : 0;
        return $user_data;
    }

    static public function dataChangeGroup($rank_data,$rank_fields){
        $backendUser = \BackendAuth::getUser();
        $adminId = $backendUser->id;
        if(get('debug') && get('admin_id')){
            $adminId = intval(get('admin_id'));
            $testUser = User::find($adminId);
            if(!empty($testUser)){
                $backendUser = $testUser;
            }
        }
        //$userGroups = UserService::getMarketGroupMembers();
        $userGroups = UserService::getGroupMembers();
        $groupIds = array_values(array_unique(array_column($userGroups,'group_id')));

        $newRankData = [];
        $curIndex = $adminId;
        if(in_array($adminId,$groupIds)){
            foreach($rank_data as $key=>$val){
                foreach($userGroups as $uid=>$info){
                    if($val['admin_id'] == $uid || $val['admin_id'] == $info['group_id']){
                        if(empty($newRankData[$info['group_id']])){
                            foreach($rank_fields as $field){
                                $newRankData[$info['group_id']][$field] = 0;
                                $newRankData[$info['group_id']]['admin_id'] = $info['group_id'];
                            }
                        }

                        foreach($rank_fields as $field){
                            $newRankData[$info['group_id']][$field] += $val[$field] ?? 0;
                        }
                        break;
                    }
                }
            }
        }elseif($backendUser->is_superuser){
            foreach($rank_fields as $field){
                $newRankData[0][$field] = 0;
                $newRankData[0]['admin_id'] = 0;
            }
            foreach($rank_data as $name_abb=>$val){
                foreach($rank_fields as $field){
                    $newRankData[0][$field] += $val[$field] ?? 0;
                }
            }
            $curIndex = 0;
        }else{

            foreach($rank_data as $k=>$val){
                //过滤组长数据
                if(in_array($val['admin_id'],$groupIds)){
                    continue;
                }
                foreach($rank_fields as $field){
                    $newRankData[$val['admin_id']][$field] = $val[$field] ?? 0;
                }
                $newRankData[$val['admin_id']]['admin_id'] = $val['admin_id'];

            }

        }
        return ['rankData'=>$newRankData,'curIndex'=>$curIndex];
    }

    /**
     * 产品线广告排名数据
     */
    static public function getOperateIndexDataLineAd(){
        $date_start = post('date_start',date('Y-m-d'));
        $date_end = post('date_end',date('Y-m-d'));
        $rank_data = self::getMarketingData('/marketing/rank',['start'=>$date_start,'end'=>$date_end,'source'=>'OPERATOR']);
        $rank_data = $rank_data['data'] ?? [];
        $admin_users = UserService::getAdminUserNames();
        $admin_users = array_column($admin_users,null,'name');
        $data = [];
        foreach($rank_data as $name=>$val){
            if(empty($admin_users[$name]['id'])){
                continue;
            }
            $data[] = [
                'admin_id' => $admin_users[$name]['id'],
                'ad_product_num' => $val['product_num'] ?? 0,
                'ad_prdouct_active_num' => $val['product_active_num'] ?? 0,
                'ad_product_paused_num' => $val['product_paused_num'] ?? 0,
            ];
        }
        $rank_fields = ['ad_product_paused_num','ad_prdouct_active_num'];
        $result = [];
        $backendUser = \BackendAuth::getUser();

        $groupData = self::dataChangeGroup($data,$rank_fields);
        $data = $groupData['rankData'];
        $curIndex = $groupData['curIndex'];


        foreach($rank_fields as $field){
            $result[$field] = self::getRankDataByField($data,$field,$curIndex);
        }
        return ['status'=>200,'data'=>$result];
    }

    //点击加购排序等
    static public function getOperateIndexDataMatomo(){
        self::init();
        $date_start = post('date_start',date('Y-m-d'));
        $date_end = post('date_end',date('Y-m-d'));
        $sendData = [

            'date_start' => $date_start,
            'date_end'   => $date_end,
            'database_name' => 'scz-' . self::$domain,
            'exposure_filter'   => 200,
        ];

        $contents = [];
        try {

            $ip = self::$ip;
            $port = 49090;
            //$ip  ='8.210.222.103';
            //$port = 46969;
            $mart = new MartClient($ip, $port);
            $returnData = $mart->productViewExec('Project2Handler','getOpPidStatsSummary',['name' => json_encode($sendData)]);

            $contents = Helper::utilToArray( $returnData['data']['data'] ?? []);

            
        } catch (\Exception $exception) {
            $message = $exception->getMessage() . PHP_EOL . $exception->getTraceAsString();
            info($message);
        }

        $result = $contents['list'] ?? [];
        $backendUser = \BackendAuth::getUser();
        $data  = [];
        $rank_fields = ['all_pid_qty','exposure_filter_pid_qty','click_pid_qty_rate','cart_pid_qty','cart_pid_qty_rate'];


        $fields = ['all_pid_qty','exposure_filter_pid_qty','cart_pid_qty','click_pid_qty'];


        $groupData = self::dataChangeGroup($result,$fields);
        $result = $groupData['rankData'];
        foreach($result as $k=>$v){
            $result[$k]['click_pid_qty_rate'] = !empty($v['all_pid_qty']) ? round($v['click_pid_qty']/$v['all_pid_qty'],4) : 0;
            $result[$k]['cart_pid_qty_rate'] = !empty($v['all_pid_qty']) ? round($v['cart_pid_qty']/$v['all_pid_qty'],4) : 0;
        }
        $curIndex = $groupData['curIndex'];


        foreach($rank_fields as $field){
            $data[$field] = self::getRankDataByField($result,$field,$curIndex);
        }
        return ['status'=>200,'data'=>$data];
    }

    /**
     * 获取目录广告数据
     */
    static public function getCataAdsData(){
        $date_start = post('date_start',date('Y-m-d'));
        $date_end = post('date_end',date('Y-m-d'));
        $page_size = intval(post('size',20));
        $product_id = intval(post('product_id',0));
        $site_id = intval(post('site_id',0));
        $sortField = post('sort','spend');
        $sortType = post('order','desc');
        $ad_name = post('ad_name','');
        $q = post('q','');
        $nq = post('nq','');

        $domains = MoresiteService::getMainSiteList();
        $domains = array_column($domains,null,'id');

        if(empty($site_id)){
            $site_id = -1;
        }
        //主域名，域名ID重置为0
        if($site_id > 0 && !empty($domains[$site_id]['is_main'])){

            $site_id = 0;
        }

        $query = DB::table('cc_marketing_data')->whereBetween('date',[$date_start,$date_end])
                 ->select(
                    'ad_name',
                    'product_id',
                    DB::raw('sum(spend) as spend'),
                    DB::raw('sum(impressions) as impressions'),
                    DB::raw('sum(clicks) as clicks'),
                    DB::raw('sum(clicks)/sum(impressions) as click_rate'),
                    DB::raw('sum(spend)/sum(clicks) as cpc'),
                    DB::raw('sum(spend)*1000/sum(impressions) as cpm')
                );
        if($site_id >= 0 ){
            $query->where('site_id',$site_id);
        }
        if(!empty($q)){
            $q_arr = explode(',',$q);
            $query->where(function($query)use($q_arr){
                foreach($q_arr as $k=>$v){
                    $query->where('ad_name','like','%'.$v.'%');
                }
            });
        }

        if(!empty($nq)){
            $nq_arr = explode(',',$nq);
            $query->where(function($query)use($nq_arr){
                foreach($nq_arr as $k=>$v){
                    $query->where('ad_name','not like','%'.$v.'%');
                }
            });
        }

        if(!empty($product_id)){
            $query->where('product_id',$product_id);
        }
        $aggQuery = clone($query);
        $agg = $aggQuery->first();
        $agg->spend = round($agg->spend,2);
        $agg->click_rate = round($agg->click_rate*100,2);
        $agg->cpc = round($agg->cpc,2);
        $agg->cpm = round($agg->cpm,2);
        $items = $query->groupBy('ad_name')->groupBy('product_id')->orderBy($sortField,$sortType)->paginate($page_size)->toArray();

        $productIds = array_column($items['data'], 'product_id');
        $products = ProductService::getProductsByIdsFromCache($productIds);
        $products = array_column($products, null, 'id');

        foreach($items['data'] as $key=>$val){
            $items['data'][$key]->f_thumb = $products[$items['data'][$key]->product_id]['f_thumb'] ?? '';
            $items['data'][$key]->product_id = $items['data'][$key]->product_id;
            $items['data'][$key]->spend = round($items['data'][$key]->spend,2);
            $items['data'][$key]->click_rate = round($items['data'][$key]->click_rate*100,2);
            $items['data'][$key]->cpc = round($items['data'][$key]->cpc,2);
            $items['data'][$key]->cpm = round($items['data'][$key]->cpm,2);
        }
        $items['status'] = 200;
        $items['agg'] = $agg;
        return $items;
    }
    /**
     * 商品产品线数据详情报告
     */

    static public function getLineProductReportOnDays(){
        self::init();
        $page_abb=post('page_abb');
        if(empty($page_abb)){
            return [];
        }
        $lines = DB::table('cc_product_sections')
            ->where('page_abb',$page_abb)
            ->select('id','admin_id','page_abb','name','code','is_line')
            ->get();
        $lines = json_decode(json_encode($lines),true);
        $lines = array_column($lines,null,'id');
        $date_start = post('date_start',date('Y-m-d',strtotime('-7 days')));
        $date_end   = post('date_end',date('Y-m-d'));
        $channel = post('channel','facebook');
        if($channel == 'facebook'){
            $lines = self::getLineReportFacebook($lines,$date_start,$date_end,$page_abb);
        }elseif($channel == 'google'){
            $lines = self::getLineReportGoogle($lines,$date_start,$date_end,$page_abb);
        }else{
            $lines = self::getLineReportAllChannel($lines,$date_start,$date_end,$page_abb);
        }

        $db_date_end = Carbon::parse($date_end)->addDays(1)->toDateString();
        //产品线优化间隔
        $line_audit_records = DB::table('cc_section_audit_records')
            ->whereBetween('submit_time',[$date_start,$db_date_end])
            ->select('id','section_id','optimize_type','submit_time')->orderBy('submit_time','asc')->get();

        foreach($lines as $k=>$line){
            $lines[$k]['optimize_sort'] = 0;
            $lines[$k]['optimize_product'] = 0;
            $lines[$k]['optimize_sort_total_interval'] = 0;
            $lines[$k]['optimize_product_total_interval'] = 0;
            foreach($line_audit_records as $record){
                if(!isset($lines[$record->section_id])){
                    continue;
                }
                if(!empty($record->optimize_type)){
                    //只调整了排序
                    if($record->section_id==$line['id'] ){
                        $lines[$k]['optimize_product'] += 1;
                    }
                    if(empty($lines[$k]['optimize_product_begin_time'])){
                        $lines[$k]['optimize_product_begin_time'] = $record->submit_time;
                    }
                    $lines[$k]['optimize_product_end_time'] = $record->submit_time;
                }
                $lines[$k]['optimize_sort'] += 1;
                if(empty($lines[$k]['optimize_sort_begin_time'])){
                    $lines[$k]['optimize_sort_begin_time'] = $record->submit_time;
                }
                $lines[$k]['optimize_sort_end_time'] = $record->submit_time;
            }
        }

        foreach($lines as $key=>$line){
            //产品线优化间隔
            if($line['optimize_sort'] > 1){
                $op_begin = $line['optimize_sort_begin_time'];
                $op_end = $line['optimize_sort_end_time'];
            }else{
                $op_begin = $date_start;
                $op_end   = $db_date_end;
            }
            $time_interval = Carbon::parse($op_begin)->diffInMinutes($op_end);

            $divisor = ($line['optimize_sort'] - 1 > 0) ? $line['optimize_sort'] - 1 : $line['optimize_sort'];
            $lines[$key]['optimize_sort_interval'] = $divisor > 0 ? round($time_interval/$divisor/60,2) : 0;
            if(!empty($line['optimize_sort'])){
                $lines[$key]['optimize_sort_total_interval'] = $time_interval;
            }
            if($line['optimize_product'] > 1){
                $op_begin = $line['optimize_product_begin_time'];
                $op_end = $line['optimize_product_end_time'];
            }else{
                $op_begin = $date_start;
                $op_end   = $db_date_end;
            }
            $time_interval = Carbon::parse($op_begin)->diffInMinutes($op_end);
            $divisor = ($line['optimize_product'] - 1 > 0) ? $line['optimize_product'] - 1 : $line['optimize_product'];
            $lines[$key]['optimize_product_interval'] = $divisor > 0 ? round($time_interval/$divisor/60,2) : 0;

            if(!empty($line['optimize_product'])){
                $lines[$key]['optimize_product_total_interval'] = $time_interval;
            }
        }
        krsort($lines);
      
        return ['status'=>200,'data'=>$lines];
    }
}
