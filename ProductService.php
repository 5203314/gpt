<?php

namespace Jason\Ccshop\Services;

use DB;
use Event;
use Illuminate\Pagination\LengthAwarePaginator;
use Jason\Ccshop\Classes\Helper;
use Jason\Ccshop\Classes\Mart\MartClient;
use Jason\Ccshop\Controllers\Advertisings;
use Jason\Ccshop\Controllers\Categories;
use Jason\Ccshop\Controllers\Currencies;
use Jason\Ccshop\Controllers\Orders;
use Jason\Ccshop\Controllers\Promotions;
use Jason\Ccshop\Controllers\ShopBase;
use Jason\Ccshop\Models\AdProductWhitelist;
use Jason\Ccshop\Models\BlindBoxSetting;
use Jason\Ccshop\Models\Category;
use Jason\Ccshop\Models\CollocationItems;
use Jason\Ccshop\Models\GoldCoinSetting;
use Jason\Ccshop\Models\IdSortRecLog;
use Jason\Ccshop\Models\Product;
use Jason\Ccshop\Models\ProductRelate;
use Jason\Ccshop\Models\ProductSku;
use Jason\Ccshop\Models\ProductPresell;
use Jason\Ccshop\Models\ProductSection;
use Jason\Ccshop\Models\ProductSectionSelected;
use Jason\Ccshop\Models\RedirectSetting;
use Jason\Ccshop\Models\RewardPoint;
use Jason\Ccshop\Models\Tag;
use Jason\Ccshop\Models\TagIcon;
use Jason\Ccshop\Models\TraceCode;
use RainLab\Translate\Classes\Translator;
use Cache;
use Cookie;
use Exception;
use Carbon\Carbon;
use Jason\Ccshop\Controllers\Searches;
use Jason\Ccshop\Models\Order;
use Jason\Ccshop\Models\OrderStatus;
use Jason\Ccshop\Models\Presale;
use Jason\Ccshop\Models\Settings;
use RainLab\Translate\Models\Locale;
use RainLab\User\Components\Account;
use Jason\Ccshop\Models\ShoppingCartValue;
use Jason\Ccshop\Models\Wishlist;
use Jason\Ccshop\Controllers\Products;
use APP;
use Jason\Ccshop\Classes\Jobs;
use Illuminate\Support\Facades\Session;
use Jason\Ccshop\Models\ProductCategory;
use Jason\Ccshop\Models\SearchRecord;
use Jason\Ccshop\Services\ImageService;
use SerenityNow\Cacheroute\Models\CacheRoute;
use Jason\Ccshop\Models\Promotion;
use Jason\Ccshop\Models\ProductsStatusChangeJob;
use Queue;
use Jason\Ccshop\Jobs\ProductStatusChangeJob;
use Backend\Facades\BackendAuth;
use Illuminate\Support\Str;
use Jason\Ccshop\Classes\DingTalk\DingTalkClient;
use GuzzleHttp\Client;
use Jason\Ccshop\Classes\ERP\Signature;
use GuzzleHttp\RequestOptions;


class ProductService
{
    public $product = null;
    public $currency = [];
    public $productPointData = [];
    public $productOriginPrice = 0;
    public $hasSetPrice = false;
    //此处字段不要修改，如需要添加字段，在调用时请动态添加
    /**
     * @var string[] 字段列表
     * 请注意，如果是二维数组，例如，tags 和 options 这种，需要列出所有的子字段，而非直接使用
     */
    public static $limitFields = [
        'id', 'admin_id', 'name', 'name2', 'status', 'list_price', 'price','max_price', 'amount', 'saled', 'popular', 'rating', 'sort',
        'discount', 'url', 'is_presell', 'promotion_price', 'f_thumb', 's_thumb', 'reviews_total',
        'wishlist_total', 'created_at', 'reward_point', 'presell_discount', 'src_price','feature_image.path',
        'rat_num', 'instock_time','cids','reviews_count', 'sales', 'thumb.path','stockpile','slug','spare_image.path',
        'weight','is_store_s3',
        // 选项信息
        'options.id', 'options.name', 'options.name2', 'options.type', 'options.sort', 'options.values.variate_value',
        'options.values.price_variate', 'options.values.id', 'options.values.name', 'options.values.name2',
        'options.values.option_id','options.values.thumb.path', 'options.values.hex',
        'tags.id', 'tags.name',
        // feed 相关字段
        'facebook_feed_thumbs','facebook_feed_thumb', 'feed_thumb',
        // 特征/值字段
        'features.name', 'features.name2', 'features.value_name',
        // sku字段
        'sku.id', 'sku.sku','sku.product_id','sku.presell','sku.sku_status','sku.option_values',
        'sku.stockpile_status','sku.store_status','sku.item_id','sku.sku_stock','sku.sku_price','sku.sku_list_price',
        //presell字段
        'presell.id', 'presell.presell_count', 'presell.start_time', 'presell.product_id', 'presell.end_time',
        'special_price'
    ];

    public static $instockStatus = ['instock', 'avlremind'];

    public static $preSaleIconBannerItems = [];

    protected static $currentSite = [];

    /**
     * 站点产品url格式
     * @var
     */
    private static $siteProductUrlFormat;

    private static $promotionBindInfo;

    private static $productTitlePrefix = null;

    private static $dbProductInfos = [];

    /**
     * 根据ids从ES获取产品
     *
     * @param array $ids
     * @param array $status
     * @param array $limitFields
     * @param array $order
     * @return array
     */
    public static function findFromEsByIds(array $ids, array $status = [], array $limitFields = [], array $order = [])
    {
        if (empty($limitFields)) {
            $limitFields = self::$limitFields;
        }

        $ids = array_filter($ids);

        $ids = array_values($ids);

        if (empty($ids)) {
            return [];
        }

        !in_array('id', $limitFields) && array_unshift($limitFields, 'id');

        try {
            $query = [
                '_source' => $limitFields,
                'query' => [
                    'bool' => [
                        'filter' => [
                            [
                                'terms' => ['id' => $ids]
                            ]
                        ],
                    ],
                ],
                'from' => 0,
                'size' => count($ids),
            ];
            if(!empty($order)){
                $query['sort'] = $order;
            }
            if (!empty($status)) {
                if (isset($status[0]) && $status[0] != 'all') {
                    $query['query']['bool']['filter'][] = [
                        'terms' => ['status' => $status],
                    ];
                }
            } else {
                $query['query']['bool']['filter'][] = [
                    'terms' => ['status' => self::$instockStatus],
                ];
            }

            $products = Searches::search($query);
            if(empty($order)){
                $products = self::sortProductsByIds($products, $ids);
            }

            if(Settings::fetch('enabled_new_product_update_strategy', 0)){

                $productStatus = !empty($status) ? $status : self::$instockStatus;
                $productOriginalIds = DB::table('cc_products')->whereIn('status',$productStatus)->whereIn('id',$ids)->lists('id');

                if (count($productOriginalIds) != count($products)) {
                    $productsIds = array_values(array_column($products, 'id'));
                    $diff = array_diff($productOriginalIds, $productsIds); //无索引的ID
                    if (empty(count($diff))) {
                        $diff = array_diff($productsIds, $productOriginalIds); //兼容下架商品不更新问题
                    }
                    if (count($diff) > 0) {
                        Jobs::putRedumpPid($diff);
                    }
                }

            }

        } catch (\Exception $e) {
            $products = [];
        }

        return $products;
    }

    /**
     * 根据ids数组排序产品
     * @param array $products
     * @param array $ids
     * @return array
     */
    public static function sortProductsByIds(array $products, array $ids)
    {
        // rsky-add 2021年1月29日 返回空数组
        if (empty($products)) {
            return [];
        }

        $products = array_column($products, null, 'id');

        $sortProducts = [];
        foreach ($ids as $key => $id) {
            if (array_key_exists($id, $products)) {
                array_push($sortProducts, $products[$id]);
            }
        }

        return $sortProducts;
    }

    /**
     * $product        为产品模型查询出来的数据
     * $purchase_price 采集价格，默认0，不传默认用产品采集价格换算，传值使用新值换算
     * @param $product
     * @param int $purchase_price
     * @param int $optionVariateValue 对选项价格换算
     * @return array 返回换算后的价格以及市场价
     * @throws Exception
     */
    public function conversionProduct($product, $purchase_price = 0, $optionVariateValue = 0)
    {
        $conversion_price = Settings::fetch('conversion_price');
        $prd_list_price_rate = Settings::fetch('prd_list_price_rate');
        $info = json_decode($conversion_price);
        if (!$info || !$prd_list_price_rate) {
            throw new Exception('请先去设置管理配置换算选项');
        }
        $source_price = isset($product->product_source['price'][1]) ? $product->product_source['price'][1] : 0;
        if (strpos($source_price, '+')) {
            $arr = explode('+', $source_price);
            $source_price = $arr[0] + $arr[1];
        }
        if (!$purchase_price) {
            $purchase_price = $product->purchase_price;
        }
        $laterOptionVariateValue = 0;
        $list_price = 0;
        $price = 0;
        $rate = 0;
        $price_variate = 0;
        $rate_arr = explode('-', $prd_list_price_rate);
        if (isset($rate_arr[0]) && isset($rate_arr[1])) {
            $rate = rand($rate_arr[0] * 100, $rate_arr[1] * 100) / 100;
        }
        if ($purchase_price == 0 && $source_price > 0) {
            $purchase_price = $source_price;
        }
        foreach ($info as $key => $val) {
            //采购价换算
            if (
                (($purchase_price >= $val->min_price) && ($purchase_price <= $val->max_price)) ||
                ($purchase_price >= $val->min_price && empty($val->max_price))
            ) {

                if ($val->extra_type == '+' && $val->extra_val > 0) {
                    $price = ($purchase_price * $val->modulus * $val->rate + $val->freight * $val->rate) + $val->extra_val;

                } elseif ($val->extra_type == '-' && $val->extra_val > 0) {
                    $price = ($purchase_price * $val->modulus * $val->rate + $val->freight * $val->rate) - $val->extra_val;

                } elseif ($val->extra_type == '*' && $val->extra_val > 0) {
                    $price = ($purchase_price * $val->modulus * $val->rate + $val->freight * $val->rate) * $val->extra_val;

                } elseif ($val->extra_type == '/' && $val->extra_val > 0) {
                    $price = ($purchase_price * $val->modulus * $val->rate + $val->freight * $val->rate) / $val->extra_val;

                } else {
                    $price = $purchase_price * $val->modulus * $val->rate + $val->freight * $val->rate;

                }

                $list_price = $price * $rate;
            }

            //选项价格换算
            if(
            (!empty($optionVariateValue) && (($optionVariateValue >= $val->min_price) && ($optionVariateValue <= $val->max_price) && $val->extra_val >= 0) ||
                ($optionVariateValue >= $val->min_price && empty($val->max_price) && $val->extra_val >= 0))
            ) {

                $optionPrice = $optionVariateValue * $val->modulus * $val->rate + $val->freight * $val->rate;

                switch($val->extra_type)
                {
                    case '+':
                        $laterOptionVariateValue = $optionPrice + $val->extra_val;
                        break;
                    case '-':
                        $laterOptionVariateValue = $optionPrice - $val->extra_val;
                        break;
                    case '*':
                        $laterOptionVariateValue = $val->extra_val > 0 ? $optionPrice * $val->extra_val : $optionPrice;
                        break;
                    case '/':
                        $laterOptionVariateValue = $val->extra_val > 0 ? $optionPrice / $val->extra_val : $optionPrice;
                        break;
                    default:
                        $laterOptionVariateValue = $optionPrice;
                        break;
                }

                $srcPrice =  \Db::table('cc_products')->where('id', $product->id)->value('price');
                $laterOptionVariateValue = $laterOptionVariateValue - $srcPrice;

                $price_variate = $laterOptionVariateValue > 0 ? '+' : '-';

                break;
            }
        }
        $arr = [
            'price' => round($price),
            'list_price' => round($list_price)
        ];
        //产品价格个位数与十位数调整
        $data = (new Products)->fromPriceDigitChange(round($price),$rate);
        $arr = [
            'price' => $data[0],
            'list_price' => round($data[1]),
            'option_variate_value' => round(abs($laterOptionVariateValue)),
            'price_variate'     => $price_variate,
        ];
        return $arr;
    }

    /**
     * 获取用户浏览历史
     * @return array
     */
    public static function getProductViewHistoriesFromEs()
    {
        $products = Cookie::get('product_view_histories', []);
        if (!is_array($products)) {
            Cookie::queue(Cookie::forget('product_view_histories'));
            return [];
        }
        if (empty($products)) {
            return [];
        }

        $ids = array_filter($products, function ($id) {
            $id = intval($id);
            return $id > 0;
        });
        if (empty($ids)) {
            return [];
        }

        //遗留问题，如果cookie存储大于设定值，重置cookie处理
        $history_cookie_number = Settings::fetch('store_view_product_histories_total') ?? 20;
        $historiesExpires = Settings::fetch('store_view_product_histories_expires') ?? 14400;
        if (intval($history_cookie_number) > 100) {
            $history_cookie_number = 100;
        }
        if (count($ids) > $history_cookie_number) {
            $ids = array_slice($ids, 0, $history_cookie_number);
            Cookie::queue('product_view_histories', $ids, $historiesExpires); //重置cookie
        }

        $ids = array_unique($ids);
        //组合购过滤福袋,盲盒,过滤组合购得勾选位 过滤穿搭
        $is_group_purchase = post('is_group_purchase',0);
        if($is_group_purchase){
            $gp_pids = CollocationItems::getCollocationItemsProductIdsAll();
            $pids = ProductSectionService::getSectionPidsBySort('iwaty-hot');
            $luckyPids = Promotions::getLuckyBagAllId();
            $blindBoxAllId = BlindBoxSetting::getBlindBoxAllId();;
            $ids = array_diff($ids, $luckyPids, $blindBoxAllId, $pids, $gp_pids);
        }
        return self::getProductsByIdsFromCache($ids, ['instock', 'stockout', 'assessment']);
    }

    /**
     * 产品内容图懒加载
     * @param $content
     * @return string|string[]|null
     */
    public static function productContentLazyload($content)
    {
        $pregRule = '/<img(.*?)src="(.*?)"(.*?)>/is';
        $pregRuleLocal = '/<img data-original="(\/storage\/.*?)\?(.*?)"(.*?)>/is';//后台上传图片删除图片清晰度参数
        $content = preg_replace_callback($pregRule, function($matches) {
            if (strpos($matches[2], '?') === false) {
                return '<img data-original="' . $matches[2] . '?x-oss-process=image/quality,q_80/format,webp" class="lazy">';
            } else {
                return '<img data-original="' . $matches[2] . '/quality,q_80/format,webp" class="lazy">';
            }
        }, $content);
        $content = preg_replace($pregRuleLocal, '<img data-original="${1}" class="lazy">', $content);
        return $content;
    }

    /**
     * 根据产品ID小时值获取促销数量 （暂时根据PID取余处理）
     * @param $pid
     * @return array
     */
    public static function randomNumberRemainingPeriod($pid)
    {
        $asiaNow = Carbon::now(config('app.timezone'));
        $currentHour = $asiaNow->hour;
        $promotionsRandomTotal = (int)(Settings::fetch('promotions_random_total'));
        if ($promotionsRandomTotal < 1) {
            $promotionsRandomTotal = 100;
        }
        $promotionsRandomNums = Settings::fetch('promotions_random_num', []);
        if(!is_array($promotionsRandomNums)){
            $promotionsRandomNums = [];
        }
        $promotionsRandomNum = [
            'start_remaining_amount' => 49,
            'end_remaining_amount' => 99,
        ];
        foreach ($promotionsRandomNums as $val) {
            if ($currentHour >= $val['start_hours'] && $currentHour <= $val['end_hours']) {
                $promotionsRandomNum = $val;
                break;
            }
        }
        $mod = ($promotionsRandomNum['end_remaining_amount'] - $promotionsRandomNum['start_remaining_amount']);
        if ($mod < 1) {
            $mod = 30;
        }
        $scene_id = SceneService::contextSceneId();
        $tmpPid = $scene_id > 0 ? $pid+$scene_id : $pid;
        $actualNum = $promotionsRandomNum['start_remaining_amount'] + ($tmpPid % $mod);
        return [
            'product_id' => $pid,
            'actual_num' => $actualNum,
            'presale' => $promotionsRandomTotal
        ];
    }
    /**
     * 根据产品ID获取促销剩余数量
     * @param $pid
     * @return mixed
     */
    public static function findPresale($pid)
    {
        $presales = self::getProductPresalesByIds([$pid]);
        return $presales[$pid] ?? [];
    }

    /**
     * 获取产品促销剩余数量数据has
     * @param array $pids
     * @return array
     */
    public static function getProductPresalesByIds(array $pids)
    {
        if (empty($pids)) {
            return [];
        }

        $cacheKey = self::getProductPresalesCackeKey();

        $redis = RedisService::getRedisDefaultClient();

        $presales = $redis->hmget($cacheKey, $pids);

        $newPresales = [];
        $hasIds = [];
        foreach ($presales as $presale) {
            if (empty($presale)) {
                continue;
            }

            $presale = json_decode($presale, true);

            $newPresales[$presale['product_id']] = $presale;
            array_push($hasIds, $presale['product_id']);
        }

        // 补全不存在的产品ID
        $completePresales = function ($presales) use ($pids) {
            $cachePids = array_column($presales, 'product_id');

            $diff = array_diff($pids, $cachePids);

            if (!empty($diff)) {
                foreach ($diff as $item) {
                    $presales[$item] = [
                        'product_id' => $item,
                        'actual_num' => 0,
                        'presale' => 50
                    ];
                }
            }

            return $presales;
        };

        $diff = array_diff($pids, $hasIds);
        if (empty($diff)) {
            return $newPresales;
        }
        $diffPresales = Presale::whereIn('product_id', $diff)
            ->Scene()
            ->select('id', 'product_id', 'actual_num', 'presale')
            ->get()
            ->toArray();

        if (empty($diffPresales)) {
            return $completePresales($newPresales);
        }

        $diffPresales = array_column($diffPresales, null, 'product_id');
        foreach ($diffPresales as $pid => $presale) {
            $newPresales[$pid] = $presale;
            $diffPresales[$pid] = json_encode($presale);
        }

        $redis->hmset($cacheKey, $diffPresales);

        return $completePresales($newPresales);
    }


    /**
     * 清理产品促销剩余数量缓存has
     * @param array $pids
     * @return bool
     */
    public static function clearHashProductPresalesByIds(array $pids)
    {
        if (empty($pids)) {
            return false;
        }
        $cacheKey = self::getProductPresalesCackeKey();
        $redis = RedisService::getRedisDefaultClient();
        foreach ($pids as $pid) {
            $redis->hdel($cacheKey, $pid);
        }
        return true;
    }
    /**
     * 获取商品促销剩余数hash key
     */
    public static function getProductPresalesCackeKey(){
        $prefix = config('cache.prefix', 'ccshop');
        $scene_id = SceneService::contextSceneId();
        $cacheKey = $prefix.':hash:product-presales-'.$scene_id;

        return $cacheKey;
    }
    /**
     * 获取最近销售订单ID
     * @param $days
     * @return array
     */
    public static function getRecentlySaledOrderIds($days)
    {
        $days = (int)$days;
        if (empty($days)) {
            return [];
        }

        $start = Carbon::now()->subDays($days)->toDateTimeString();
        $end = Carbon::now()->toDateTimeString();

        return Order::query()
            ->whereIn('status_id', OrderStatus::saledStatusIds())
            ->whereBetween('created_at', [$start, $end])
            ->lists('id');
    }

    /**
     * 从产品hash表中批量获取数据
     * @param array $ids
     * @param array $status
     * @return array
     */
    public static function getProductsByIdsFromCache(array $ids, array $status = []): array
    {
        if (empty($ids)) {
            return [];
        }

        $cacheKey = self::getProductCacheKey();

        $redis = RedisService::getRedisPersistentClient();

        $products = $redis->hmget($cacheKey, $ids);

        $newProducts = [];
        $hasIds = [];
        foreach ($products as $product) {
            if (empty($product)) {
                continue;
            }

            $product = json_decode($product, true);

            $newProducts[] = $product;
            array_push($hasIds, $product['id']);
        }

        $diff = array_diff($ids, $hasIds);

        if (get('debug_pro_hash') === 'true' && !empty($diff)) {
            info('HASH中获取产品的数据为空: ' . json_encode($diff));
        }

        // // 重建产品缓存
        // $diffProducts = self::rebuildProductCache($diff);

        // if (empty($diffProducts)) {
        //     return self::packageProducts($newProducts, $ids, $status);
        // }

        // $newProducts = array_merge($newProducts, $diffProducts);
        return self::packageProducts($newProducts, $hasIds, $status);
    }

    /**
     * 组装产品数据
     * @param $products
     * @param $ids
     * @param $status array  状态筛选['instock']
     * @return array
     */
    private static function packageProducts($products, $ids, array $status = []): array
    {
        $sceneId = SceneService::contextSceneId();
        $saveField = ['name','slug','f_thumb','price','list_price','promotion_price','discount'];
        $siteId = MoresiteService::getCurrentSiteId();

        if ($sceneId) {
            $sceneDatas = MultiSceneProductService::getProductsSceneByIdsFromCache($ids, $sceneId);
        }

        // 子域名图片处理
        $siteItems = MoresiteService::getSiteInfo();

        $allPromotionInfo = PromotionService::getAllActiveProductPromoInfo();

        foreach ($products as $key => $product) {
            /**
             * @author rsky <renxing@shengyuntong.net>
             * @datetime timestamp 2021年1月29日
             * @description 如果存在状态筛选，则对数据做处理
             */
            if (!empty($status) && !in_array($product['status'], $status)) {
                unset($products[$key]);

                continue;
            }

            if(isset($allPromotionInfo[$product['id']])){
                $product['promo_info'] = json_decode($allPromotionInfo[$product['id']],true);
            }

            if (empty($product)) {
                continue;
            }
            $product['tag_icons'] = ProductTagIconService::getProductTagIcons($product['id']);
            // 组装产品图片
            $products[$key] = self::packageProductImages($product);
            if (!empty($sceneId)) {
                foreach ($saveField as $val) {
                    if (isset($sceneDatas[$product['id']][$val]) && !empty($sceneDatas[$product['id']][$val])) {
                        if ($val == 'f_thumb' && !empty($siteItems) && !empty($siteItems['bucket_domain']) ) {
                            $products[$key][$val] = \Event::fire(
                                'jason.ccshop.getThumb',
                                [$sceneDatas[$product['id']][$val], 'thumb', $siteItems],
                                true
                            );
                            continue;
                        }
                        $products[$key][$val] = $sceneDatas[$product['id']][$val];
                    }
                }
                $slug =  $sceneDatas[$product['id']]['slug'] ?? $product['slug'] ?? 'product';
                $products[$key]['url'] = self::generateUrl($product['id'], $slug , $siteId);
            }else{
                $slug =  $product['slug'] ?? 'product';
                $products[$key]['url'] = self::generateUrl($product['id'], $slug , $siteId);
            }
        }
        $products = self::packageProductsCategoryName($products);
        // 根据原始ID对产品数据进行排序
        return self::sortProductsByIds($products, $ids);
    }


    /**
     * 组装产品图片数据
     * @param array $data
     * @param bool $isContainsContent 是否包含产品内容图
     * @return array
     */
    public static function packageProductImages(array $data, bool $isContainsContent = false): array
    {
        //商品店铺信息
        $data['business'] = BusinessService::onGetProductBusiness($data['id']);

        // 排除未启用OSS存储的站点
        if (!MoresiteService::isEnabledOSS()) {
            return $data;
        }

        // 子域名图片处理
        self::$currentSite = $siteItems = MoresiteService::getSiteInfo();

        if (!empty($siteItems) && !empty($siteItems['bucket_domain'])) {
            $data = \Event::fire(
                'jason.ccshop.getThumb',
                [$data, 'products', $siteItems],
                true
            );
        }

        if ($isContainsContent && !empty($data['content'])) {
            $data['content'] = \Event::fire('jason.ccshop.getThumb', [$data['content'], 'product-contents', $siteItems], true);
        }

        return $data;
    }

    /**
     * 删除redis hash 产品数据 <产品删除或产品状态更改为非在库和非到货提醒时, 会触发删除>
     * @param $ids
     */
    public static function deleteProductsFromCache($ids)
    {
        $redis = RedisService::getRedisPersistentClient();

        if (!is_array($ids)) {
            $ids = [$ids];
        }

        $proCacheKey = self::getProductCacheKey();

        foreach ($ids as $pid) {
            // 删除产品hash缓存
            $redis->hdel($proCacheKey, $pid);
        }

        MultiSceneProductService::deleteProductSceneCacheData($ids);
    }

    /**
     * 删除redis hash 产品状态数据<产品删除时，删除状态数据>
     */
    public static function deleteProductStatusFromCache($ids)
    {
        $redis = RedisService::getRedisPersistentClient();

        if (!is_array($ids)) {
            $ids = [$ids];
        }

        $proStatusCacheKey = self::getProductStatusCacheKey();

        foreach ($ids as $pid) {
            // 删除产品hash缓存
            $redis->hdel($proStatusCacheKey, $pid);
        }
    }

    /**
     * 获取产品缓存KEY
     * @return string
     */
    protected static function getProductCacheKey()
    {
        $local = Translator::instance()->getLocale();

        $prefix = config('cache.prefix', 'ccshop');

        return $prefix.':hash:products-'.$local;
    }

    /**
     * 获取产品状态缓存KEY
     * @return string
     */
    protected static function getProductStatusCacheKey()
    {
        $local = Translator::instance()->getLocale();

        $prefix = config('cache.prefix', 'ccshop');

        return $prefix.':hash:product-status-'.$local;
    }


    /**
     * 缓存产品数据到redis
     * @param $ids
     * @return array
     */
    public static function rebuildProductCache($ids): array
    {
        if (!is_array($ids)) {
            if (is_numeric($ids)) {
                $ids = [$ids];
            } else {
                return [];
            }
        }

        $products = self::findFromEsByIds($ids, self::$instockStatus);

        if (empty($products)) {
            return [];
        }

        $products = array_column($products, null, 'id');

        $newProducts = [];
        foreach ($products as $id => $product) {
            $product = self::transFieldsType($product);
            $product = self::rebuildProductAppend($product);
            $newProducts[$id] = $product;
            $products[$id] = json_encode($product, JSON_UNESCAPED_UNICODE);
        }

        $cacheKey = self::getProductCacheKey();
        $redis = RedisService::getRedisPersistentClient();
        $redis->hmset($cacheKey, $products);

        return $newProducts;
    }

    /**
     * 转换产品部分字段类型
     *
     * @param $product
     * @return mixed
     */
    private static function transFieldsType($product)
    {
        $transFloat = [
            'price', 'src_price', 'list_price', 'promotion_price', 'max_price', 'rat_num'
        ];

        $transInt = [
            'rating', 'popular', 'amount', 'sales', 'is_presell', 'stockpile', 'sort', 'reviews_count', 'saled'
        ];

        foreach ($transFloat as $val) {
            if (isset($product[$val])) {
                $product[$val] = floatval($product[$val]);
            }
        }

        foreach ($transInt as $val) {
            if (isset($product[$val])) {
                $product[$val] = intval($product[$val]);
            }
        }

        return $product;
    }

    /**
     * 缓存产品hash数据字段数据附加处理
     * @param $product
     * @return mixed
     */
    private static function rebuildProductAppend($product)
    {
        if (isset($product['name'])) {
            $productTitlePrefix = self::$productTitlePrefix;
            if ($productTitlePrefix == null) {
                $productTitlePrefix = self::$productTitlePrefix = Settings::fetch('productTitlePrefix', '');
            }
            $product['name'] = $productTitlePrefix . $product['name'];
        }
        // 转换产品在库时间为当前系统时区
        if (!empty($product['instock_time'])) {
            $product['instock_time'] = Carbon::createFromFormat(
                'Y-m-d H:i:s',
                $product['instock_time'],
                'UTC')
                ->setTimezone(config('app.timezone'))
                ->toDateTimeString();
        }
        $skuIntervalPrice = self::getIntervalSkuPrice($product['options'] ?? []);
        $product['sku_min_price'] = $skuIntervalPrice['sku_min_price'];
        $product['sku_max_price'] = $skuIntervalPrice['sku_max_price'];
        return $product;
    }

    /**
     * 从产品options里面提取sku价格段
     * @param $productOptions
     * @return int[]
     */
    public static function getIntervalSkuPrice($productOptions)
    {
        $sku_price=[
            'sku_min_price' => 0,
            'sku_max_price' => 0,
        ];
        if(!is_array($productOptions)){
            return $sku_price;
        }
        try {
            foreach ($productOptions as $option) {
                $optionCollect = collect($option['values'] ?? [])->filter(function ($item) {
                    //改价大于0且属于+-价的
                    return $item['variate_value'] > 0 && in_array($item['price_variate'], ['+', '-']);
                })->groupBy('price_variate');
                if($optionCollect->isEmpty()){
                    continue;
                }
                $plusOptionCollect = $optionCollect->get('+' , []);
                $lessenOptionCollect = $optionCollect->get('-' , []);
                if(!empty($lessenOptionCollect)){
                    $sku_price['sku_min_price'] -= $lessenOptionCollect->pluck('variate_value')->max();
                }
                if(!empty($plusOptionCollect)){
                    $sku_price['sku_max_price'] += $plusOptionCollect->pluck('variate_value')->max();
                }
            }
        }  catch (Exception $e) {
            info('SKU价格段计算异常:' . $e->getMessage());
        }
        return $sku_price;
    }
    /**
     * Notes: 按字段更新产品缓存数据
     * User: ma
     * Date: 2021/8/2
     * Time:18:22
     * @param $datas[
     * 'pid1' => ['name'=>'xxx','price'=>123]
     * 'pid2' => ['name'=>'xxx','price'=>123]
     * ]
     */
    public static function rebuildProductCacheByFields($datas)
    {
        $cacheKey = self::getProductCacheKey();
        $redis = RedisService::getRedisStoreClient();

        $pids = array_keys($datas);

        if (empty($pids)) {
            return true;
        }

        $productDatas = $redis->hmget($cacheKey, $pids);

        $saveData = [];
        foreach ($productDatas as $productData) {
            $productData = self::transFieldsType($productData);
            $productData = json_decode($productData, 1);
            $productId = data_get($productData, 'id');
            if (empty($productData) || empty($productId)) {
                continue;
            }
            $newData = array_merge($productData, $datas[$productId]);
            $saveData[$productId] = json_encode($newData);
        }

        if ($saveData) {
            $redis->hmset($cacheKey, $saveData);
        }
        return true;
    }

    /**
     * 重建产品状态缓存
     * @param array $data .e.g: ['product_id' => 'product_status'],...]
     */
    public static function rebuildProductStatusCache(array $data)
    {
        $cacheKey = self::getProductStatusCacheKey();
        $redis = RedisService::getRedisStoreClient();

        if (empty($data)) {
            return false;
        }

        $redis->hmset($cacheKey, $data);
        return true;
    }

    /**
     * 通过产品数据缓存产品
     * @param array $data
     */
    public static function rebuildProductCacheFromData(array $data)
    {
        $product = [
            'id' => $data['id']
        ];

        $dataDot = array_dot($data);
        $dataDotKeys = array_keys($dataDot);

        $filterKeys = [];
        $limitFields = self::$limitFields;

        foreach ($dataDotKeys as $dotKey) {
            if (in_array($dotKey, $limitFields)) {
                $filterKeys[] = $dotKey;
                continue;
            }

            $dotKeyArr = explode('.', $dotKey);
            if (count($dotKeyArr) == 1) {
                continue;
            }

            $dotKeyArr = array_filter($dotKeyArr, function ($val) {
                return !is_numeric($val);
            });

            $newDotKey = implode('.', $dotKeyArr);
            if (in_array($newDotKey, $limitFields)) {
                $filterKeys[] = $dotKey;
            }
        }

        if (empty($filterKeys)) {
            return;
        }

        foreach ($filterKeys as $filterKey) {
            if (isset($dataDot[$filterKey])) {
                array_set($product, $filterKey, $dataDot[$filterKey]);
            }
        }

        $product = self::transFieldsType($product);

        $product = \Event::fire('jason.ccshop.getThumb', [$product, 'products', MoresiteService::getMainDomain()], true);
        $product = self::rebuildProductAppend($product);
        $cacheKey = self::getProductCacheKey();
        $redis = RedisService::getRedisPersistentClient();
        $redis->hset($cacheKey, $product['id'], json_encode($product, JSON_UNESCAPED_UNICODE));
    }



    /**
     * 组装产品价格
     * @param $products
     * @return array
     */
    private static function buildProductPrice($products)
    {
        $products = array_column($products, null, 'id');

        $ids = array_keys($products);

        $prices = self::getProductPricesByIds($ids);

        foreach ($products as $id => $product) {
            if (empty($prices[$id])) {
                continue;
            }

            $products[$id] = array_merge($product, $prices[$id]);
        }

        return $products;
    }

    /**
     * 获取产品价格
     * @param array $ids
     * @return array
     */
    public static function getProductPricesByIds(array $ids)
    {
        if (empty($ids)) {
            return [];
        }

        $getPrice = function ($ids) {
            $prices = [];

            $modelProducts = Product::whereIn('id', $ids)->select('id', 'price', 'list_price')->get();

            foreach ($modelProducts as $product) {
                $prices[$product->id]['id'] = $product->id;
                $prices[$product->id]['price'] = !empty($product->price) ? $product->price : 9999;
                $prices[$product->id]['list_price'] = !empty($product->list_price) ? ceil($product->list_price) : 9999;

                $discount = !empty($product->discount) ? $product->discount : 0;
                $discount = $discount == '49%' ? '50%' : $discount;
                $prices[$product->id]['discount'] = $discount;

                $productArr = $product->toArray();
                $prices[$product->id]['promotion_discount'] = ShopBase::getPromotionDiscount($productArr);
            }

            $dbProducts = DB::table('cc_products')->whereIn('id', $ids)->select('id', 'price', 'list_price')->get();

            foreach ($dbProducts as $product) {
                $prices[$product->id]['id'] = $product->id;
                $prices[$product->id]['src_price'] = !empty($product->price) ? $product->price : 9999;
            }

            return $prices;
        };

        $prefix = config('cache.prefix', 'ccshop');

        $cacheKey = $prefix.':hash:product-prices';

        $redis = RedisService::getRedisDefaultClient();

        $prices = $redis->hmget($cacheKey, $ids);

        $newPrices = [];
        $hasIds = [];
        foreach ($prices as $price) {
            if (empty($price)) {
                continue;
            }

            $price = json_decode($price, true);

            $newPrices[$price['id']] = $price;
            array_push($hasIds, $price['id']);
        }

        $diff = array_diff($ids, $hasIds);

        if (empty($diff)) {
            return $newPrices;
        }

        $diffPrices = $getPrice($diff);

        if (empty($diffPrices)) {
            return $newPrices;
        }

        foreach ($diffPrices as $key => $price) {
            $newPrices[$price['id']] = $price;
            $diffPrices[$key] = json_encode($price);
        }

        $redis->hmset($cacheKey, $diffPrices);

        return $newPrices;
    }
    /**
     * 根据关键词搜索推荐词
     */
    public static function recomSearchTerms($keyword,$options=[]){
        if(empty($options)){
            $options = ['cNum'=>5,'pNum'=>5];
        }
        $categories = Cache::tags('categories')->remember('categories-search-terms', 2568, function (){
            $data = \Jason\Ccshop\Models\Category::where('is_enabled',1)
                ->select('name','name2','id','slug')->orderBy('nest_left','asc')->get()->toArray();
            return $data;
        });
        $searchTerms = [];
        $cNum = $options['cNum'];
        $pNum  = $options['pNum'];
        $termNum = 0;
        //推荐分类
        foreach($categories as $category){
            if($termNum >= $cNum){
                break;
            }
            if(str_contains($category['name'],$keyword)){
                $searchTerms[] = ['title'=>$category['name'],'url'=>$category['url'],'type'=>'category'];
                $termNum++;
            }
        }
        $pNum = $cNum + $pNum - $termNum;

        //推荐商品
        $searchProducts = self::searchProducts($keyword,[],['sales'=>'desc'],['new_search_weight'=>true,'page'=>1,'first_page_sort'=>true,'limitFields'=>['id','name','url','options','sales']]);
        if(!empty($searchProducts['data'])){
            $products = array_slice($searchProducts['data'], 0,$pNum);
            foreach($products as $product){
                $searchTerms[] = ['title'=>$product['name'],'url'=>$product['url'],'type'=>'product'];
            }
        }
        return $searchTerms;
    }
    /**
     * 根据关键词搜索产品PIDS
     * @param $keyword
     * @param array $range 搜索结果的查询范围，默认为空，例：
     * 例1：['price'=>'0~999','create_at'=>'2018-12-01~2019-01-01']
     * 例2：['price'=>'~999','create_at'=>'2018-12-01~2019-01-01']
     * 例3：['price'=>'9999~','create_at'=>'2018-12-01~2019-01-01']
     * @param array $sort 搜索结果的排序方式，默认为空，例：[['sort'=>'desc']]
     * @param array $options  operator 搜索词的拆分方式,默认为词组拆分的or
     *                       fields 指定搜索的字段
     *                       min_score 指定搜索最小的相关性得分
     * @param array $cids
     * @param int $sceneId
     * @param array $shieldCids   屏蔽的分类ids
     * @param array $bigPagination  大分页参数  from size
     * @return array
     */
    public static function searchPids($keyword, $range = [], $sort = [], $options = [],$cids = [], $sceneId = 0, $shieldCids = [], $bigPagination = []){
        $returnData = [
            'total'=>0,
            'pids'=>[],
        ];
        if (empty($keyword)) {
            return $returnData;
        }
        $siteId = MoresiteService::getCurrentSiteId();
        if (!$sceneId){
            $sceneId = SceneService::contextSceneId();
        }

        // 如果搜索的是数字，则直接返回[pid]
        if (is_numeric($keyword)) {
            $siteInfo =current(MoresiteService::getSitesBySiteIds([$siteId]));
            if (!empty($siteInfo['conceal']) && empty($siteInfo['is_main'])){
                $keyword -= $siteInfo['conceal'];//真实pid
            }
            $returnData['pids'] = [$keyword];
            $returnData['total'] = 1;
            return $returnData;
        }
        //查询字段
        $fields = ['id'];
        //新搜索权重,只搜索商品、分类、颜色、尺码标题
        if(!empty($options['new_search_weight']) && empty($options['fields'])){
            $options['fields'] = ["name^10","categories.name^10","options.values.name^8"];
            $fields[] = 'categories';
        }
        //es 最大数量限制
        $fromSizeMax = 10000;
        $query = [
            '_source' =>  $fields,
            'query' => [
                'bool' => [
                    'must' => [
                        [
                            'multi_match' => [
                                'query' => $keyword,
                                'type' => "best_fields",
                            ]
                        ],
                    ],
                    'filter' => [
                        'terms' => [
                            'status' => ["instock", "avlremind"]
                        ]
                    ]
                ],
            ],
            'from' => 0,
            'size' => $fromSizeMax,
        ];
        if(isset($bigPagination['from']) && isset($bigPagination['size'])){
            $fromMax = ($fromSizeMax - $fromSizeMax%$bigPagination['size']);//计算大分页最大from
            $query['from'] = ($fromMax < $bigPagination['from']) ? $fromMax:$bigPagination['from'];
            $sizeMax = $fromSizeMax - $query['from'];//大分页最后的size
            $query['size'] = ($sizeMax < $bigPagination['size']) ? $sizeMax:$bigPagination['size'];
        }
        if (isset($query['query']['bool']['must'][0]['multi_match'])){
            $multiMatch = &$query['query']['bool']['must'][0]['multi_match'];
            if (isset($options['operator'])){
                $multiMatch['operator'] = $options['operator'];
            }
            if (isset($options['fields'])){
                $multiMatch['fields'] = $options['fields'];
            }else{
                $multiFields = [
                    "name^10", "name2^9", "page_title", "content", "meta_keywords", "meta_description",
                    "features.name", "features.value_name^8", "categories.name^9",
                    "categories.page_title", "options.values.name^8",
                    // "categories.meta_keywords", "categories.description",
                    // "categories.meta_description",
                ];
                if ($sceneId){
                    $sceneFields = ["scene_data.name^20", "scene_data.name2^19", "scene_data.features.name", "scene_data.features.value_name^18"];
                    $multiFields = array_merge($multiFields, $sceneFields);
                }
                $multiMatch['fields'] = $multiFields;
            }
        }
        if (!empty($range)) {
            self::formatRangeQueries($range, $query['query']['bool']['must']);
        }
        if (isset($options['min_score'])) {
            $query['min_score'] = $options['min_score'];
        }
        if($cids){
            $query['query']['bool']['must'][]['terms']['cids'] = is_array($cids) ? $cids : [$cids];
        }
        //屏蔽的分类ids
        if($shieldCids){
            $query['query']['bool']['must_not'][]['terms']['cids'] = is_array($shieldCids) ? $shieldCids : [$shieldCids];
        }
        if($sceneId){
            $query['query']['bool']['must'][] = [
                "nested"=>[
                    "path" => "scene_data",
                    "query" => [
                        "bool" => [
                            "must" => [
                                [
                                    "term" => [
                                        "scene_data.scene_id" => $sceneId
                                    ]
                                ],
                            ]
                        ]
                    ]
                ]
            ];
        }
        if (empty($sort)) {
            $sort = self::formatSortQueries($keyword);
        }
        if (isset($sort['instock_time']) || isset($sort['price']) || isset($sort['sales']) || isset($sort['reviews_total']) || isset($sort['id'])){
            $query['sort'] = $sort;
            $query['sort']['_score'] = 'desc';
        }else{
            $query['sort']['_score'] = 'desc';
            $query['sort'] = array_merge($query['sort'], $sort);
        }
        $query['sort'] = array_filter($query['sort']);//删除sort空值字段，用于某些不需要相关度排序的情况
        $query = Searches::handleQueryVersion($query);
        $response = Searches::search($query, ['original' => true]);
        if (get('debug_test') == 'search_time') {
            info('ES搜索结果：' . json_encode(['took' => $response['took'] ?? 'false', '_shards' => $response['_shards'] ?? []]));
        }
        if (empty($response['hits']['total'])) {
            return $returnData;
        }
        $returnData['total'] = ($fromSizeMax < $response['hits']['total']) ? $fromSizeMax : $response['hits']['total'];
        $returnData['pids'] = array_pluck($response['hits']['hits'],'_source.id');
        return $returnData;
    }

    /**
     * 根据参数对$pids进行复杂重排
     * @param $pids array
     * @param $sortOption array 排序方式
     * 可选排序值：
     *      saled:['saled'=>[15(天数)]], 默认倒序 全排
     *      sales:['sales'=>[15(天数)]], 默认倒序
     * @return array
     */
    public static function complexSortPids(array $pids, array $sortOption): array
    {
        if(empty($pids)){
            return [];
        }
        $sort = key($sortOption);
        $option = $sortOption[$sort] ?? [];
        if(!is_array($option)){
            $option = [$option];
        }
        switch ($sort){
            case 'sales'://按x天数下单量重排
            case 'saled'://按x天数销量重排
                if(empty($option[0])){
                    return $pids;
                }
                $days = $option[0] ?? 15;
                $orderStartDate = Carbon::today()->subDay($days)->startOfDay();
                $orderEndDate = Carbon::today()->endOfDay();

                $newPids = [];
                $type = ($sort == 'saled') ? 'saled' : 'placed';
                if (in_array($days, [7, 15, 30]) && self::checkRealSalesRedisIsset($type)) {
                    $newPids = self::getProductRealSales($pids, $days, $type);
                } else {
                    $serviceTimeZone = config('app.timezone');
                    $statusIds = ($sort == 'saled') ? OrderStatus::saledStatusIds() : OrderStatus::placedStatusIds();//下单量跟销量的状态
                    $saledPids = (new Orders())->getOrderPaidOrPlacedQty($pids,$orderStartDate,$orderEndDate,$serviceTimeZone,$statusIds);
                    foreach($pids as $pid){
                        $newPids[$pid] = $saledPids[$pid]['value'] ?? 0;
                    }
                }
                arsort($newPids);
                $pids = array_keys($newPids);
                break;
        }
        return $pids;
    }
    /**
     * 根据关键词搜索产品
     * @param $keyword
     * @param array $range 搜索结果的时间日期范围，默认为空，例：['~999','2018-12-01~2019-01-01']
     * @param array $sort 搜索结果的排序方式，默认为空，例：[['sort'=>'desc']]
     * @param array $options operator 搜索词的拆分方式,默认为词组拆分的or
     *                       fields 指定搜索的字段
     *                       min_score 指定搜索最小的相关性得分
     * @param array $shieldCids   屏蔽的分类ids
     * @return array
     */
    public static function searchProducts($keyword, $range = [], $sort = [], $options = [],$cids = [], $sceneId = 0, $shieldCids = [])
    {
        if (empty($keyword)) {
            return [];
        }

        $perPage = Settings::fetch('product_list_display_num_per_page', 20);
        $page = !empty($options['page']) ? $options['page'] : (get('page', 1) ?: 1);
        $site_id = data_get(MoresiteService::getCurrentSiteInfo(), 'id') ?: 0;
        if ($page == 1) {
            $ip = request()->getClientIp();
            $user_agent = \Request::server("HTTP_USER_AGENT");
            SearchRecord::firstOrCreate(compact('keyword', 'site_id', 'ip', 'user_agent'));
        }

        if (!$sceneId){
            $sceneId = SceneService::contextSceneId();
        }

        if (empty($sort)) {
            $sort = self::formatSortQueries($keyword);
        }
        $complexSort = $sort['complexSort'] ?? [];//复杂排序参数
        unset($sort['complexSort']);
        //搜索关键词pids
        $bigPage = 5;
        //es 最大数量限制
        $fromSizeMax = 10000;
        $pageMax = (int)ceil($fromSizeMax / $perPage);//计算最后一页有效页码
        $bigPerPage = $perPage * $bigPage;
        $currentPage = LengthAwarePaginator::resolveCurrentPage();
        $currentPage = ($pageMax < $currentPage) ? $pageMax : $currentPage;//大于最后一页页码，改为显示最后一页
        $bigCurrentPage = (int)(ceil($currentPage / $bigPage));
        $bigPagination['from'] = ($bigCurrentPage - 1) * $bigPerPage;
        $bigPagination['size'] = $bigPerPage;
        $cacheKey = md5(json_encode([$keyword, $range, $sort, $options, $cids, $sceneId, $site_id,$shieldCids,$complexSort,$bigPagination]));

        //搜索结果缓存30分钟
        $searchData = \Cache::tags('search-keyworld')->remember($cacheKey, 30, function () use($keyword, $range, $sort, $options, $cids, $sceneId, $shieldCids,$complexSort,$bigPagination) {
            $searchData = self::searchPids($keyword, $range, $sort, $options, $cids, $sceneId, $shieldCids, $bigPagination);
            //复杂排序
            if (!empty($searchData['pids']) && !empty($complexSort)) {
                $searchData['pids'] = self::complexSortPids($searchData['pids'], $complexSort);
            }
            return $searchData;
        });

        // pids分页start
        $pidsCurrentPage = ($currentPage - 1 - ($bigCurrentPage - 1) * $bigPage) * $perPage;
        $currentPageResults = array_slice($searchData['pids'], $pidsCurrentPage, $perPage);
        $result = new LengthAwarePaginator($currentPageResults, $searchData['total'], $perPage);
        $result->setPath('/search/' . $keyword);
        $data = $result->toArray();
        $params = get();
        if(!empty($params)){
            //页码链接拼接get参数
            $result->appends($params);
        }
        $data['render'] = $result->render();
        // pids分页end

        $data['data'] = self::getProductsByIdsFromCache($data['data'], ['instock']);

        //默认排序时 首页排序使用sales排序：30天下单量
        if (!empty($options['first_page_sort']) && $page == 1 && empty($sort)) {
            $data['data'] = collect($data['data'])->sortByDesc('sales')->toArray();
        }
        if(empty($data['data']) && $page == 1){
            $data['total'] =  0;
        }
        return $data;
    }

    /**
     * 将传入的字符类型的数据格式化为适用es查询的数组数据
     * @param $rangeQueries
     * @param $searchQueries
     */
    public static function formatRangeQueries($rangeQueries, &$searchQueries)
    {
        $rangeKeys = array_keys($rangeQueries);
        foreach ($rangeKeys as $rangeKey) {
            if(str_contains($rangeQueries[$rangeKey],'~')){
                $rangeList = explode('~', $rangeQueries[$rangeKey]);
                if (!empty($rangeList[0])) {
                    $rangeArr[$rangeKey]['gte'] = $rangeList[0];
                }
                if (!empty($rangeList[1])) {
                    $rangeArr[$rangeKey]['lte'] = $rangeList[1];
                }
            }elseif(str_contains($rangeQueries[$rangeKey],'^')){
                $rangeList = explode('^', $rangeQueries[$rangeKey]);
                if (!empty($rangeList[0])) {
                    $rangeArr[$rangeKey]['gt'] = $rangeList[0];
                }
                if (!empty($rangeList[1])) {
                    $rangeArr[$rangeKey]['lt'] = $rangeList[1];
                }
            }
        }
        if (isset($rangeArr)) {
            $searchQueries[] = ['range' => $rangeArr];
        }
    }

    /**
     * 兼容之前的功能，之前的功能会将&order拼接在keyword参数中，将order拆分出来用于排序
     * @param $keyword
     * @return array
     */
    public static function formatSortQueries(&$keyword)
    {
        $sort = [];
        $paramKeyWord = mb_strtolower($keyword);
        $keyword = mb_strstr($paramKeyWord, '&order=', true);

        if ($keyword === false) {
            $keyword = $paramKeyWord;
        } else {
            $orderBy = mb_strstr($paramKeyWord, '&order=');
            if (!empty($orderBy)) {
                $orderBy = trim(substr($orderBy, 7));
                if (in_array($orderBy, [
                    'price_desc', 'price_asc', 'popular_desc', 'newness_desc', 'saled_desc',
                    'sales_desc', 'sort_desc', 'sort_asc', 'instock_desc'
                ])) {
                    $orderByArr = explode('_', $orderBy);
                }
            }
        }

        if (isset($orderByArr)) {
            if ($orderByArr[0] === 'newness') {
                $orderByArr[0] = 'created_at';
            }elseif ($orderByArr[0] === 'instock'){
                $orderByArr[0] = 'instock_time';
            }
            $sort = [$orderByArr[0] => $orderByArr[1]];
        }
        return $sort;
    }

    /**
     * @param $type 订单类型（默认saled（已付款）placed（已下单）
     * @param int $day 距离今天几天前的订单数（默认0（所有）,可选 7，15,30）
     * @param int $categories 分类ID 0为所有
     * @param string $order desc/asc
     * @param int $limit 一页多少数据
     * @param array $pida id_sort的ID
     * @param array sort  二三次排序字段
     * @return array
     */
    public static function getCategoriesProductOrderAmount(
        $type,
        $day = 0,
        $categories = 0,
        $order = 'desc',
        $limit = 60,
        $pida = [],
        $sort = []
    ) {
        if ($categories == 0) {
            $statsModel = DB::table('cc_product_order_stats')
                ->select('cc_product_order_stats.*')
                ->join('cc_products', 'cc_products.id', '=', 'cc_product_order_stats.product_id')
                ->whereNotIn('cc_product_order_stats.product_id', $pida)
                ->where('cc_product_order_stats.type', $type)
                ->where('cc_product_order_stats.day', $day)
                ->orderBy('cc_product_order_stats.amount', $order);

        } else {
            $statsModel = DB::table('cc_product_order_stats')
                ->select('cc_product_order_stats.*')
                ->join('cc_products', 'cc_products.id', '=', 'cc_product_order_stats.product_id')
                ->join('cc_product_categories', 'cc_product_categories.product_id', '=',
                    'cc_product_order_stats.product_id')
                ->where('cc_product_categories.category_id', '=', $categories)
                ->whereNotIn('cc_product_order_stats.product_id', $pida)
                ->where('cc_product_order_stats.type', $type)
                ->where('cc_product_order_stats.day', $day)
                ->orderBy('cc_product_order_stats.amount', $order);
        }
        if (empty($sort)) {
            $sort = ['sort' => 'desc', 'product_id' => 'desc', 'order_created_at' => 'desc'];
        }
        $statsModel = self::formatSortData($statsModel, $sort);
        $pageDate = $statsModel->paginate($limit);
        $data = [];
        if (isset($pageDate->toArray()['data'])) {
            $ids = array_column($pageDate->toArray()['data'], 'product_id');
            $products = self::getProductsByIdsFromCache($ids);
            $data['page'] = $pageDate;
            $data['products'] = $products;

        }

        return $data;
    }
    /**
     * 产品排序
     * @param $model  模型
     * @param array $sort 排序字段
     */
    public static function formatSortData($model, $sort)
    {
        foreach ($sort as $key => $value) {
            switch ($key) {
                case 'sort':
                    $model = $model->orderBy('cc_products.sort', $value);
                    break;
                case 'order_created_at':
                    $model = $model->orderBy('cc_product_order_stats.order_created_at', $value);
                    break;
                case 'product_id':
                    $model = $model->orderBy('cc_product_order_stats.product_id', $value);
                    break;
                default:
                    $model = $model->orderBy($key, $value);
                    break;
            }
        }
        return $model;
    }

    /**
     * 获取最近销量高的产品
     * @param $days
     * @param int $limit
     * @param array $cids
     * @param array $excludeIds
     * @param bool $isPlaced  是否按下单量获取商品
     * @param array $excludeCids 需要排除分类id
     * @param array $dateRange  指定日期区间 ['Y-m-d', 'Y-m-d']|['2021-10-01', '2021-1-30']
     * @return array
     */
    public static function getRecentlySaledProducts($days, $limit = 600, array $cids = [],array $excludeIds = [], $isPlaced = false, $excludeCids = [], $dateRange = [])
    {
        $order = [];
        if ($isPlaced) {
            $order['field'] = 'order_id';
        }

        $products = self::getRecentlySaledProductIds($days, $limit, $cids, $excludeIds, $order, $dateRange, $excludeCids);

        $pids = array_keys($products);

        $perPage = post('perPage',0);
        if($perPage == 0){
            $perPage = Settings::fetch('product_list_display_num_per_page', 60);
        }

        $pageProducts = Helper::manualPaging($pids, ['perPage' => $perPage]);

        if (!empty($pageProducts['data'])) {
            $pageProducts['data'] = self::getProductsByIdsFromCache($pageProducts['data']);
        } else {
            $pageProducts['data'] = [];
        }

        return $pageProducts;
    }

    /**
     * 获取最近销售的产品ID
     * @param $days
     * @param int $limit
     * @param array $cids
     * @param array $excludeIds
     * @param array $order
     * @param string $dateRange 指定日期区间 ['Y-m-d', 'Y-m-d']|['2021-10-01', '2021-1-30'] rsky-add
     * @param array $excludeCids 需要排除分类id
     * @return mixed
     */
    public static function getRecentlySaledProductIds($days, $limit = 600, $cids = [], $excludeIds = [], $order = [], $dateRange = [], $excludeCids = [])
    {
        $limit = (int)$limit;

        $keyStr = $days.$limit.json_encode($cids);

        // rsky-add 2021-2-20 存在指定结束日期，将其放入缓存key值，以作区分
        if ($dateRange) {
            $keyStr = $keyStr . implode('--', $dateRange);
        }

        $cacheKey = md5($keyStr);

        if (isset($order['field']) && $order['field'] == 'order_id') {
            $tags = 'recently-placed-product-ids';
            $statusIds = OrderStatus::placedStatusIds();
        } else {
            $tags = 'recently-saled-product-ids';
            $statusIds = [];
        }

        return Cache::tags($tags)->remember(
            $cacheKey, 360,
            function () use ($days, $limit, $cids, $excludeIds, $statusIds, $order, $dateRange, $excludeCids) {
                $productSales = OrderService::getRecentlySaledProductIdsFromEs($days, $limit, $cids, $statusIds, $order, $dateRange, $excludeCids);
                $pids = array_keys($productSales);

                $pids = array_diff($pids,$excludeIds);

                $query = DB::table('cc_products')
                    ->whereIn('cc_products.id', $pids)
                    ->where('cc_products.status', 'instock');

                if(count($cids) > 0){
                    $query->join('cc_product_categories','cc_products.id','=','cc_product_categories.product_id');
                    $query->whereIn('cc_product_categories.category_id',$cids);
                }
                $instockPids = $query->lists('cc_products.id');

                $instockSales = [];
                foreach ($instockPids as $pid) {
                    if (array_key_exists($pid, $productSales)) {
                        $instockSales[$pid] = $productSales[$pid];
                    }
                }
                $keys = array_keys($instockSales);

                $vals = array_values($instockSales);

                array_multisort($vals,SORT_DESC, $keys,SORT_ASC);

                $instockSales = array_combine($keys, $vals);

                return $instockSales;
            }
        );
    }

    /**
     * 根据产品N天内销量重排$pids
     * @param array $pids 产品ids
     * @param array $cids 指定分类销量
     * @param int $recentlySaledDay 销量排序天数
     * @param int $recentlySaledLimit 销量排序数量 默认 0 全部重排序
     * @param int $limit N天内销量数量 默认1000
     * @return array ['pid'=>销量]
     */
    public static function sortRecentlySaledProductIds(array $pids, array $cids = [],$recentlySaledDay=15,$recentlySaledLimit=0,$limit=1000)
    {
        if(!is_array($pids) || empty($pids)){
            return [];
        }
        $pids = array_fill_keys($pids, 0);
        $recentlySaledDayPids = ProductService::getRecentlySaledProductIds($recentlySaledDay, $limit, $cids);
        $recentlySaledDayNewPids = array_intersect_key($recentlySaledDayPids, $pids);
        if($recentlySaledLimit>0 && count($recentlySaledDayNewPids)>$recentlySaledLimit){
            $recentlySaledDayNewPids = array_slice($recentlySaledDayNewPids, 0,$recentlySaledLimit,true);
        }
        return $recentlySaledDayNewPids + $pids;
    }
    /**
     * 获取id_sort产品
     * @return array
     */
    public static function getIdSortProducts()
    {
        $result = [
            'adproducts' => [],
            'pida' => []
        ];
        //当前URL包含了Google广告的落地页，置顶参数变更
        $current_path = $_SERVER['REQUEST_URI'];
        $google_ad_word = '/(gclid|gbraid|wbraid|google|adwords|doubleclick.net)/i';
        $number_of_words_in_my_path  = preg_match_all($google_ad_word, $current_path);
        $pids = get('id_sort', '');
        if(!$pids&&$number_of_words_in_my_path>0){
            if(get('k')&&get('v')){
                $extra=[];
                $params=App::make('Jason\Ccshop\Components\Catalog')->sendUrlParams($extra);
                $matomo_request=App::make('Jason\Ccshop\Components\Catalog')->getRecommendRelatedProductsResult($params);
                if(!is_array($matomo_request)&&$matomo_request){
                    $matomo_request=json_decode($matomo_request,true);
                }
                $key=isset($params['k']) ? $params['k'] : '';
                if(isset($matomo_request[$key])&&isset($params['k'])){
                    $pids=$matomo_request[$key];
                }
            }
        }

        $uniqueId = get('uniqueId', '');
        if (empty($pids) && empty($uniqueId)) {
            return $result;
        }

        $pids = urldecode($pids);
        //去除前后空格
        $pidas = explode(',', $pids);
        //id_sort 投放的是伪装ID，须转为真实ID

        $siteId = MoresiteService::getCurrentSiteId();
        $siteInfo =current(MoresiteService::getSitesBySiteIds([$siteId]));

        $conceal = 0;
        if(!empty($siteInfo['conceal']) && empty($siteInfo['is_main'])){
            $conceal =  $siteInfo['conceal'];
        }
        foreach($pidas as $k=>$pid){
            $pidas[$k] = Intval($pid) - $conceal;
        }
        // 组合unique置顶数据
        $uniquePids = RedirectSetting::getIdSorts($uniqueId);
        $pidas = array_merge($pidas, $uniquePids);
        $pida = [];
        foreach($pidas as $pid){
            $pida[] = (int)trim($pid);
        }
        $pida = array_unique(array_filter($pida));
        $beforeCount = count($pida);
        $pida = collect($pida)->filter(function ($item) {
            return is_numeric($item) && $item < 2147483647;
        })->toArray();
        $afterCount = count($pida);
        if($beforeCount > $afterCount){
            $_url = request()->fullUrl();
            info('id置顶投放信息有误，来源url：'.$_url.PHP_EOL);
        }
        $result['pida'] = $pida;
        $result['adproducts'] = self::getProductsByIdsFromCache($pida);
        $result['adproducts'] = self::packageProductsCategoryName($result['adproducts']);
        $result['adproducts'] = self::handleSortFeatureImage($result['adproducts'],$pidas);
        return $result;
    }

    /**
     * 获取产品最近销售时间
     * @param $id
     * @return string|null
     */
    public static function getSaledLatelyTime($id)
    {
        $id = (int)$id;

        if (empty($id)) {
            return '';
        }

        $newestOrder = Product::getSaledLatelyOrder($id);
        $lastTime = $newestOrder['created_at'] ?? '';
        if(!empty($lastTime)){
            return Helper::timeConvert($lastTime,'UTC',config('app.timezone', 'UTC'));
        }else{
            return '';
        }

    }

    /**
     * 获取产品预售数据
     * @param $pid
     * @return mixed
     */
    public static function findProductPresell($pid)
    {
        return Cache::tags('product-presells')->remember($pid, 1440, function () use ($pid) {
            $pid = (int)$pid;
            $presell = ProductPresell::where('product_id', $pid)->first();
            if (empty($presell)) {
                return [];
            }

            return $presell->toArray();
        });
    }

    /**
     * 获取最近X天/小时内上架商品ID
     * @param $limit
     * @param array $cids
     * @param $time
     * @param $timeType $time类型
     * @return array
     */
    public static function getNewlyInstockIds($limit, array $cids = [], $time = 0, $timeType = 'days')
    {
        $limit = (int)$limit;
        if ($limit <= 0) {
            return [];
        }

        $cacheKey = md5($time . $timeType . $limit . json_encode($cids));

        return Cache::tags('newly-instock-product-ids')->remember($cacheKey, 180, function () use ($limit, $cids, $time, $timeType) {
            $query = DB::table('cc_products')->where('cc_products.status', 'instock');

            if (!empty($cids)) {
                $query = $query->join(
                    'cc_product_categories', 'cc_product_categories.product_id', '=', 'cc_products.id'
                )->groupBy('cc_products.id')->whereIn('cc_product_categories.category_id', $cids);
            }
            if ($time > 0){
                switch($timeType)
                {
                    case 'hours':
                        $start = Carbon::now()->subHour($time)->toDateTimeString();
                        break;
                    case 'days':
                    default:
                        $start = Carbon::now()->subDays($time)->toDateTimeString();
                        break;
                }
                $end = Carbon::now()->toDateTimeString();
                $query = $query->whereBetween('cc_products.instock_time', [$start, $end]);
            }
            return $query->orderBy('cc_products.instock_time', 'desc')
                ->orderBy('cc_products.id', 'desc')
                ->limit($limit)
                ->lists('cc_products.id');
        });
    }

    /**
     * 获取最近上架商品
     * @param int $limit
     * @param array $cids
     * @param int $days
     * @param bool $isPage
     * @return array
     */
    public static function getNewlyInstockPage($limit = 300, array $cids = [], $days = 0,$isPage = true)
    {
        $limit = (int)$limit;
        if ($limit <= 0) {
            return [];
        }
        $productIds = self::getNewlyInstockIds($limit, $cids, $days);
        if ($isPage) {
            $perPage = Settings::fetch('product_list_display_num_per_page', 60);
            $perPage = $perPage ? $perPage : 60;
            $result = Helper::manualPaging($productIds, ['perPage' => $perPage]);
            $products = self::getProductsByIdsFromCache($result['data'], ['instock']);
            $result['data'] = $products;
            return $result;
        } else {
            return self::getProductsByIdsFromCache($productIds, ['instock']);
        }
    }
    /**
     * 通过es过滤条件搜索产品
     * @param array $filter es的搜索条件 例：['terms' => ['cids' => 17]]
     * @param array $limitFields 指定查出来的字段
     * @param array $range 搜索结果的时间日期范围，默认为空，例：['~999','2018-12-01~2019-01-01']
     * @param array $sort 搜索结果的排序方式，默认为空，例：[['sort'=>'desc']]
     * @param int $size 获取产品数，默认为0，则是获取设置的产品数
     * @param int $featureId 产品特征值，默认为0
     * @return array
     */
    public static function getProductsByFilterFromEs($filter, $limitFields = [], $range = [], $sort = [], $size = 0, $featureId = 0)
    {
        if ($size === 0) {
            $size = Settings::fetch('product_list_display_num_per_page', 20);
        }
        if (empty($limitFields)) {
            $limitFields = self::$limitFields;
        }
        $query = [
            '_source' => $limitFields,
            'query' => [
                'bool' => [
                    'filter' => $filter
                ],
            ],
            'from' => 0,
            'size' => $size
        ];
        if(!empty($featureId)){
            $query['query']['bool']['must'] = [
                "nested"=>[
                    "path" => "features",
                    "query" => [
                        "bool" => [
                            "must" => [
                                [
                                    "term" => [
                                        "features.value_id" => $featureId
                                    ]
                                ],
                            ]
                        ]
                    ]
                ]
            ];
        }
        $discountSort = false;
        if (array_key_exists('discount', $sort)) {
            $discountSort = $sort['discount'];
            unset($sort['discount']);
        }
        $wishSort = false;
        if (array_key_exists('wishlist_total', $sort)) {
            $wishSort = $sort['wishlist_total'];
            unset($sort['wishlist_total']);
        }

        if (!empty($sort)) {
            $query['sort'] = $sort;
        }
        if (!empty($range)) {
            self::formatRangeQueries($range, $query['query']['bool']['must']);
        }
        $products = Searches::search($query);
        if ($discountSort !== false) {
            self::sortProductsFromEs($products, 'discount', $discountSort);
        }
        if($wishSort !== false){
            self::sortWishProductsFromEs($products);
        }

        $ids = array_column($products, 'id');
        return self::packageProducts($products,$ids);
    }

    /**
     * [sortWishProductsFromEs description]
     * @param  [type] &$products [description]
     */
    public static function sortWishProductsFromEs(&$products){

        $customFieldsWhis = array_column($products,null,'custom_fields');
        array_multisort($customFieldsWhis,SORT_DESC,$products);

    }

    /**
     * 将从es查出来的产品排序
     * @param $products
     * @param string $field 需要排序的字段
     * @param string $sort 排序的方式
     */
    public static function sortProductsFromEs(&$products, $field, $sort)
    {
        usort($products, function ($first, $second) use ($sort, $field) {
            $firstProduct = trim($first[$field], '%');
            $secondProduct = trim($second[$field], '%');
            if ($firstProduct == $secondProduct) {
                return 0;
            }
            if ($sort == 'desc') {
                return ($firstProduct > $secondProduct) ? -1 : 1;
            } elseif ($sort == 'asc') {
                return ($firstProduct > $secondProduct) ? 1 : -1;
            }
        });
    }

    /**
     * 根据产品ID判断该产品是否允许被当前用户评论
     *
     * @param $id 产品ID
     * @return bool
     */
    public static function isAllowComments($id) : bool
    {
        is_string($id) && $id = (int) $id;

        $userId = (new Account)->user()->id ?? null;

        if(empty($id) || is_null($userId)){
            return false;
        }



        $cacheKey = 'isAllowComments-' . $userId;
        $orderData = \Cache::tags("user-buy-count")->remember($cacheKey, 1440, function () use ($userId) {

            $receivedGoodsStatusId = OrderStatus::where('code','confirmreceipt')->pluck('id');
            return \DB::table('cc_orders')->join('cc_order_products', 'cc_order_products.order_id', '=', 'cc_orders.id')
                ->where(['cc_orders.uid' => $userId, 'cc_orders.status_id' => $receivedGoodsStatusId])
                ->select(DB::raw('sum(qty) as sales, product_id'))
                ->groupBy('cc_order_products.product_id')
                ->lists('sales', 'cc_order_products.product_id');
        });


        return isset($orderData[$id]);
    }

    /**
     * 将订单状态数据以HASH形式存储到redis中
     *
     * @param $redis redis连接实例
     * @return void
     */
    public static function storageOrderStatusByHash($redis)
    {
        $orderStatus = OrderStatus::select('id','code','name','label_class','mail_template','is_default','is_sendmail')->get()->toArray();

        $orderStatus = array_column($orderStatus,null,'code');

        $data = [];

        array_walk($orderStatus,function($item,$index) use (&$data){
            $data[$index] = json_encode($item,JSON_UNESCAPED_UNICODE);
        });

        if(empty($data)){
            return;
        }

        $prefix = config('cache.prefix', 'ccshop');

        $cacheKey = $prefix.':hash:order-status';

        $redis->hmset($cacheKey,$data);
    }

    /**
     * 更新es中指定产品id的部分产品数据
     *
     * @param $productId
     * @param $partProduct
     *
     */
    public static function updateProductEs($productId, array $partProduct) {
        try {
            $es = Searches::esInstance();
            $langs = Locale::listEnabled();
            $esIndexName = Searches::getSearchEngineIndexName();
            foreach ($langs as $code => $lang) {
                $indexFullName = $esIndexName.'-'.$code;
                $es->update([
                    'index' => $indexFullName,
                    'type' => 'products',
                    'id' => $productId,
                    'refresh' => 'wait_for',
                    'body' => [
                        'doc' => $partProduct
                    ]
                ]);
            }
        } catch (\Exception $e) {
            \Log::info('更新产品[ '.$productId.' ] 数据异常: '.$e->getMessage());
            return false;
        }
        return true;
    }

    /**
     * 更新产品ID场景部分数据
     * @param $productId
     * @param array $scenesData
     * @param int|null $sceneId
     * @return bool
     */
    public static function updateProductSceneDataEs($productId,array $scenesData,int $sceneId = null) {
        try {
            if ($sceneId === null) {
                $sceneId = SceneService::contextSceneId();
            }
            $productsSceneData = ProductService::findFromEsByIds([$productId], ['all'],['scene_data']);
            $saveData = false;
            foreach (($productsSceneData[0]['scene_data'] ?? []) as $key => $pSceneData) {
                if(!isset($pSceneData['scene_id']) || $pSceneData['scene_id'] != $sceneId){
                    continue;
                }
                $saveData = true;
                $productsSceneData[0]['scene_data'][$key] = array_merge($pSceneData,$scenesData);
            }
            if($saveData){
                $es = Searches::esInstance();
                $langs = Locale::listEnabled();
                $esIndexName = Searches::getSearchEngineIndexName();
                foreach ($langs as $code => $lang) {
                    $indexFullName = $esIndexName.'-'.$code;
                    $es->update([
                        'index' => $indexFullName,
                        'type' => 'products',
                        'id' => $productId,
                        'refresh' => 'wait_for',
                        'body' => [
                            'doc' => $productsSceneData[0]
                        ]
                    ]);
                }
            }
        } catch (\Exception $e) {
            \Log::info('更新产品[ ' . $productId . ' ]场景[' . $sceneId . ']部分数据,异常: [' . $e->getMessage().'],更新数据:'.json_encode($scenesData));
            return false;
        }
        return true;
    }
    /**
     * 更新es中指定产品id的部分产品数据
     *
     * @param $productId
     * @param $partProduct
     *
     */
    public static function batchUpdateProductEs(array $partProducts) {
        try {
            $es = Searches::esInstance();
            $langs = Locale::listEnabled();
            $esIndexName = Searches::getSearchEngineIndexName();
            $params['body'] = [];
            foreach ($langs as $code => $lang) {
                $indexFullName = $esIndexName.'-'.$code;
                foreach ($partProducts as $productId =>$partProduct) {
                    $params['body'][] = [
                        'update' => [
                            '_index' => $indexFullName,
                            '_type' => 'products',
                            '_id' => $productId,
                        ]
                    ];
                    $params['body'][] = [
                        'doc' => $partProduct
                    ];
                }
                $es->bulk($params);
            }
        } catch (\Exception $e) {
            \Log::info('批量更新ES数据异常: '.$e->getMessage());
            return false;
        }
        return true;
    }

    /**
     * 批量更新产品ID场景部分数据
     * @param array $scenesDatas['pid'=>['name'=>'xxx']]
     * @param int|null $sceneId
     * @return bool
     */
    public static function batchUpdateProductSceneDataEs($scenesDatas,int $sceneId) {
        if (empty($sceneId)) {
            return false;
        }
        try {
            $pids = array_keys($scenesDatas);
            if (empty($pids)) {
                return false;
            }
            $productsSceneDatas = ProductService::findFromEsByIds($pids, ['all'],['scene_data']);
            $saveData = [];
            foreach ($productsSceneDatas as $productData) {
                $productId = data_get($productData,'id',0);
                if (empty($productId)) {
                    continue;
                }
                $productSceneData = $productData['scene_data'] ?? [];
                foreach($productSceneData as $key => $pSceneData) {
                    if(!isset($pSceneData['scene_id']) || $pSceneData['scene_id'] != $sceneId){
                        continue;
                    }
                    $productData['scene_data'][$key] = array_merge($pSceneData,$scenesDatas[$productData['id']]);
                }
                $saveData[$productId] = ['scene_data' => $productData['scene_data']];
            }
            self::batchUpdateProductEs($saveData);
        } catch (\Exception $e) {
            \Log::info('批量更新产品场景[' . $sceneId . ']部分数据,异常: [' . $e->getMessage().'],更新数据:'.json_encode($scenesDatas));
            return false;
        }
        return true;
    }
    /**
     * 更新最新下单商品
     */
    public static function getRealTimeOrderProduct()
    {
        try {
            $realTimeOrderProductsFilePath = storage_path('app/media/real_time_order_products.txt');

            $nowTimeI = date('i');
            $real_time_products_old = [];
            $cacheNowTimeI = 0;
            if(file_exists($realTimeOrderProductsFilePath)){
                $realTimeOrderProductsDatas = json_decode(file_get_contents($realTimeOrderProductsFilePath),true);

                $real_time_products_old = $realTimeOrderProductsDatas['productDatas']??[];
                $cacheNowTimeI = (int)$realTimeOrderProductsDatas['nowTimeI']??0;
            }






            if($nowTimeI != $cacheNowTimeI ){
                $real_time_product_set = Settings::fetch('real_time_product_set',[]);
                $real_time_product_set_setup = [];
                if($real_time_product_set){
                    $real_time_product_set_setup = json_decode($real_time_product_set,true);
                }

                $nowTimeH = date('H');
                $relation_config = [];
                foreach($real_time_product_set_setup as $config){
                    if($config['time_start'] <= $nowTimeH && $config['time_end'] >= $nowTimeH){
                        $relation_config = $config;break;
                    }
                }
                if(!$relation_config){
                    return ;
                }


                $datetimeNow = Carbon::now();
                $end = $datetimeNow -> toDateTimeString();
                $start = $datetimeNow -> subSeconds(300) -> toDateTimeString();


                $q = [
                    'query' => [
                        'bool' => [
                            'must' => [
                                [
                                    'range' => [
                                        'created_at' => [
                                            'gte' => $start,
                                            'lte' => $end,
                                            'format' => 'yyyy-MM-dd HH:mm:ss',
                                            'time_zone' => config('app.timezone')
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ],
                    "from" => 0, "size" =>30,
                    'sort' => [
                        'created_at' => 'desc'
                    ],
                    '_source' => [
                        'id',
                        'sn',
                        'created_at',
                        'products.product_id',
                        'products.thumb',
                        'products.url',
                        'products.name',
                    ],
                ];

                $options = ['suffix' => 'order', 'type' => 'orders'];
                $orders = Searches::search($q, $options);


                $real_time_products_old_ids = array_column($real_time_products_old,'id');
                $real_time_products_old_ctime = array_column($real_time_products_old,'c_time');
                $real_time_products_new = [];
                $order_time = Carbon::parse($end) -> format('m/d H:i');
                foreach ($orders as $ordersVal) {
                    foreach ($ordersVal['products'] as $products) {
                        if(!isset($products['product_id']) || empty($products['product_id']) || (in_array($products['product_id'],$real_time_products_old_ids) && in_array($ordersVal['created_at'],$real_time_products_old_ctime))){
                            continue;
                        }
                        $real_time_products_new[] = [
                            'id' => $products['product_id'],
                            'f_thumb' => $products['thumb'],
                            'url' => str_replace(request()->getSchemeAndHttpHost(),'', $products['url']),
                            'name' => $products['name'],
                            'order_time' => $order_time,
                            'c_time' => $ordersVal['created_at']
                        ];

                    }

                }

                $isGetProductSection = false;
                $isGetProductSectionRand = rand(1, 100);
                if ($relation_config['percent'] >= $isGetProductSectionRand) {
                    $isGetProductSection = true;
                }

                if ($isGetProductSection && count($real_time_products_new) < $relation_config['realProductNum']) {
                    $all_hot_sale_section = \App::make('Jason\Ccshop\Components\Catalog')->ProductSection($relation_config['code']);
                    if (count($all_hot_sale_section['items'])) {
                        $hot_sale_randnum = rand($relation_config['getNum_min'], $relation_config['getNum_max']);
                        $all_hot_sale = [];
                        $all_hot_sale_all = $all_hot_sale_section['items'];
                        $all_hot_sale_num = count($all_hot_sale_all);
                        if($all_hot_sale_num > $hot_sale_randnum){
                            $all_hot_sale_keys = array_rand($all_hot_sale_all,$hot_sale_randnum);
                            if(is_array($all_hot_sale_keys)){
                                foreach($all_hot_sale_keys as $val){
                                    $all_hot_sale[]=$all_hot_sale_all[$val];
                                }
                            }else{
                                $all_hot_sale[] = $all_hot_sale_all[$all_hot_sale_keys];
                            }
                        }

                        $all_hot_sale_res = [];
                        foreach ($all_hot_sale as $all_hot_sale_item) {
                            $all_hot_sale_res[] = [
                                'id' => $all_hot_sale_item['id'],
                                'url' => str_replace(request()->getSchemeAndHttpHost(),'', $all_hot_sale_item['url']),
                                'f_thumb' => $all_hot_sale_item['f_thumb'],
                                'name' => $all_hot_sale_item['name'],
                                'order_time' => $order_time
                            ];
                        }

                        $real_time_products_new = array_merge($all_hot_sale_res, $real_time_products_new);
                    }
                }



                if(empty($real_time_products_old)){
                    $real_time_products_res = $real_time_products_new;
                }else{
                    $real_time_products_res = array_merge($real_time_products_new,$real_time_products_old);
                }

                if(count($real_time_products_res) > 60){
                    $real_time_products_res = collect($real_time_products_res)->map(function ($item, $key) {
                        if($key < 60){
                            return $item;
                        }
                    }) -> all();
                    $real_time_products_res = array_filter($real_time_products_res);
                }

                $cacheData = [
                    'productDatas' => $real_time_products_res,
                    'nowTimeI' => $nowTimeI,
                ];
                file_put_contents($realTimeOrderProductsFilePath,json_encode($cacheData),LOCK_EX);
            }
        } catch (\Exception $e) {
            info('实时出单商品 ERROR :'.json_encode($e->getMessage()));
        }
    }
    /**
     * 批量获取产品的收藏数（公式计算）
     * @param $productIds
     * @return array
     */
    public static function  getProductWishCountByProductIds($productIds)
    {
        $redis = RedisService::getRedisDefaultClient();
        $prefix = config('cache.prefix', 'ccshop');
        $cacheKey = $prefix.":"."RecentSaleNumProducts:".$productIds[0].":".$productIds[count($productIds)-1].":".count($productIds);
        $recentProductSaleNums =  Cache::remember($cacheKey, 1440, function () use ($productIds) {
            return  OrderService::getRecentlySaleNumFromEsByProductIds($productIds);
        });
        $recentProductCartNums = self::_getRecentProductAddCartCountByProductIds($redis,$productIds);
        $realWishProductNums =self::_getRecentProductWishCountByProductIds($redis,$productIds);
        $result = [];
        foreach ($productIds as $productId) {
            $realSaleNum = !empty($recentProductSaleNums[$productId])?intval($recentProductSaleNums[$productId]):0;
            $recentProductCartNums = !empty($recentProductCartNums[$productId])?intval($recentProductCartNums[$productId]):0;
            $realWishProductNums = !empty($realWishProductNums[$productId])?intval($realWishProductNums[$productId]):0;
            $random = self::_generateRandomByProductId($productId);
            $wishCount =   $realWishProductNums + $realSaleNum + $recentProductCartNums * $random['x'] + ceil($random['a'] * $random['b']/100);
            $result[$productId] = intval($wishCount);
        }
        return $result;
    }

    /**
     * 批量获取产品的销量(公式计算)
     * @param $productIds
     * @return array
     */
    public static function getProductSaleCountByProductIds($productIds)
    {
        $redis = RedisService::getRedisDefaultClient();
        $cacheKey = "RecentSaleNumProducts:".$productIds[0].":".$productIds[count($productIds)-1].":".count($productIds);
        $recentProductSaleNums =  Cache::remember($cacheKey, 1440, function () use ($productIds) {
            return  OrderService::getRecentlySaleNumFromEsByProductIds($productIds);
        });
        $recentProductCartNums = self::_getRecentProductAddCartCountByProductIds($redis,$productIds);
        $result = [];
        foreach ($productIds as $productId){
            $realSaleNum = !empty($recentProductSaleNums[$productId])?intval($recentProductSaleNums[$productId]):0;
            $recentProductCartNums = !empty($recentProductCartNums[$productId])?intval($recentProductCartNums[$productId]):0;
            $random = self::_generateRandomByProductId($productId);
            $result[$productId] =  $realSaleNum+$recentProductCartNums * $random['y'] + $random['a'];
        }
        return $result;
    }

    /**
     * 批量获取产品收藏数
     * @param $redis
     * @param $productIds
     * @param int $days
     * @return array|false
     */
    private static function _getRecentProductWishCountByProductIds($redis,$productIds,$days = 30)
    {
        $prefix = config('cache.prefix', 'ccshop');
        $productIdCount = count($productIds);
        $cacheKey = '';
        if($productIdCount == 1){
            $cacheKey = $prefix.":"."ProductAddedWishNums:".$productIds[0];
        }elseif ($productIdCount > 1) {//多个产品id
            $cacheKey = $prefix.":"."ProductAddedWishNums:".$productIds[0].":".$productIds[count($productIds)-1].":".count($productIds);
        }
        if($redis->exists($cacheKey)){
            return array_combine($productIds,$redis->hmget($cacheKey,$productIds));
        }
        $start = Carbon::now()->subDays($days)->toDateTimeString();
        $end = Carbon::now()->toDateTimeString();
        $recentWishCount = Wishlist::select(DB::raw('count(product_id) as amount,product_id'))
            ->whereBetween('created_at',[$start,$end])
            ->groupBy('product_id')
            ->get();
        $productNums = [];
        if(!$recentWishCount->isEmpty()){
            $productNums = array_column($recentWishCount->toArray(),null,'product_id');
        }
        $storeData = [];
        foreach ($productIds as $productId){
            $storeData[$productId] = isset($productNums[$productId])?$productNums[$productId]['amount']:0;
        }
        $redis->hmset($cacheKey,$storeData);
        $redis->expire($cacheKey,86400);
        return $storeData;
    }

    /**
     * 批量获取产品加购数据
     * @param $redis
     * @param $productIds
     * @param int $days
     * @return array|false
     */
    private static function _getRecentProductAddCartCountByProductIds($redis,$productIds,$days = 30)
    {
        $prefix = config('cache.prefix', 'ccshop');
        $productIdCount = count($productIds);
        $cacheKey = '';
        if($productIdCount == 1){
            $cacheKey = $prefix.":"."ProductAddedCartNums:".$productIds[0];
        }elseif ($productIdCount > 1) {//多个产品id
            $cacheKey = $prefix.":"."ProductAddedCartNums:".$productIds[0].":".$productIds[count($productIds)-1].":".count($productIds);
        }
        if($redis->exists($cacheKey)){
            return array_combine($productIds,$redis->hmget($cacheKey,$productIds));
        }
        $start = Carbon::now()->subDays($days)->toDateTimeString();
        $end = Carbon::now()->toDateTimeString();
        $recentProductAddCartCount = ShoppingCartValue::select(DB::raw('sum(qty) as amount,product_id'))
            ->whereBetween('created_at',[$start,$end])
            ->groupBy('product_id')
            ->get();
        $productNums = [];
        if(!$recentProductAddCartCount->isEmpty()){
            $productNums = array_column($recentProductAddCartCount->toArray(),null,'product_id');
        }
        $storeData = [];
        foreach ($productIds as $productId){
            $storeData[$productId] = isset($productNums[$productId])?$productNums[$productId]['amount']:0;
        }
        $redis->hmset($cacheKey,$storeData);
        $redis->expire($cacheKey,86400);
        return $storeData;
    }

    /**
     * 生产产品随机因子
     * @param $productId
     * @return array
     */
    private static function _generateRandomByProductId($productId)
    {
        $random_x = range(10,18);
        $random_y = range(8,10);
        $random_a = range(0,50);
        $random_b = range(150,200);
        $index_x = intval(($productId %  (count($random_x) - 1)));
        $index_y = intval(($productId %  (count($random_y) - 1)));
        $index_a = intval(($productId %  (count($random_a) - 1)));
        $index_b = intval(($productId %  (count($random_b) - 1)));
        $x = $random_x[$index_x];
        $y = $random_y[$index_y];
        $a = $random_a[$index_a];
        $b = $random_b[$index_b];
        $result = ['x'=>$x,'y'=>$y,'a'=>$a,'b'=>$b];
        return $result;
    }

    /**
     * 谷歌链接转换 首图转最后一张特色图
     */

    public static function handleFeatureImage(array $data){
        if (empty($data)) return [];
        //链接是否为谷歌投放链接,修改列表图为特色图最后一张
        $source = get('source', '');
        if(empty($source)){
            $source = isset($_COOKIE['urlSource']) ? $_COOKIE["urlSource"] : '';
        }
        if($source == 'google'){
            setcookie("urlSource", $source);
            foreach ($data as &$value) {
                if (!empty($value['feature_image']) && count($value['feature_image']) > 0) {
                    $lastFeatureImage = end($value['feature_image']);
                    $value['f_thumb'] = !empty($lastFeatureImage['path']) ? $lastFeatureImage['path'] : $value['f_thumb'];
                }
            }
        }
        return $data;
    }

    /**
     * 谷歌-置顶产品-首图转最后一张特色图
     * @param array $data
     * @param $sort_ids  置顶产品
     * @return array
     */

    public static function handleSortFeatureImage(array $data,array $sort_ids){
        if (empty($data) || empty($sort_ids)) return [];
        $source = get('sort_source', '');
        if($source == 'show'){
            foreach ($data as &$value) {
                if (!empty($value['feature_image']) && count($value['feature_image']) > 0) {
                    $lastFeatureImage = end($value['feature_image']);
                    $value['f_thumb'] = !empty($lastFeatureImage['path']) ? $lastFeatureImage['path'] : $value['f_thumb'];
                }
            }
        }
        return $data;
    }

    /**
     * 通过sku展示列表产品
     * @param array $ids 产品ID数据
     * @param string $isSort 是否进行sku内部排序 SORT_DESC/SORT_ASC
     * @return array
     */
    public static function getSkuProductList(array $ids, string $isSort = 'asc'): array
    {
        $items = [];
        // 合并置顶广告ID
        if ($adProducts = self::getIdSortProducts() && !empty($adProducts['pida'])) {
            $ids = array_values(array_unique(array_merge($adProducts['pida'], $ids)));
        }
        // 过滤ID
        $ids = array_map(function ($id) {
            return (int)trim($id);
        }, $ids);

        if (empty($ids)) {
            return $items;
        }

        $cacheKey = md5(serialize($ids));
        $productOptionValueIdArr = \Cache::remember($cacheKey, 1440, function () use ($ids, $isSort) {
            // 产品选项表通过product_id 和 name 筛选出产品对应的选项值
            $result = \DB::table('cc_product_options as op')
                ->select('op.product_id as product_id', 'op.id as option_id', 'opval.id as option_value_id', 'opval.sort as sort')
                ->whereIn('product_id', $ids)
                ->where(function($query) {
                    $query->where('op.name', 'カラー')->orWhere('op.name2', '颜色');
                })
                ->whereNull('op.deleted_at')
                ->leftJoin('cc_product_option_values as opval', 'op.id', '=', 'opval.option_id')
                ->whereNull('opval.deleted_at')
                ->get();

            // 获取在库的sku选项信息
            $pids = collect($result)->pluck('product_id')->toArray();
            $proSkuItems = \DB::table('cc_product_skus')
                ->select('product_id', 'option_values')
                ->whereIn('product_id', $pids)
                ->where('sku_status', 1)
                ->get();

            $proSkuInfo = [];
            foreach ($proSkuItems as $sku) {
                // 获取sku选项信息
                $optionValues = json_decode($sku->option_values, true);
                if (!$optionValues || !is_array($optionValues)) {
                    continue;
                }
                foreach ($optionValues as $val) {
                    $optionValueID = current(array_keys($val));
                    if (isset($proSkuInfo[$sku->product_id][$optionValueID])) {
                        continue;
                    }
                    $proSkuInfo[$sku->product_id][$optionValueID] = true;
                }
            }

            /**
            组装产品信息
            [
            '选项值ID' => [
            'product_id' => 1,
            'option_id' => 111
            ],
            ...
            ]
             */
            $items = [];
            foreach ($result as $item) {
                // 选项值是否为在库sku
                if (!isset($proSkuInfo[$item->product_id][$item->option_value_id])) {
                    continue;
                }
                $items[$item->option_value_id] = [
                    'product_id' => $item->product_id,
                    'option_id' => $item->option_id,
                    'option_value_id' => $item->option_value_id,
                    'sort' => $item->sort
                ];
            }
            // $ids为勾选位排序完成的结果, 上面连表查询后, 得出的数据结果则又会变为无序, 需要再次依据 $ids 进行排序
            usort($items, function($a,$b) use($ids) {
                return (array_search($a['product_id'], $ids) < array_search($b['product_id'], $ids)) ? -1 : 1;
            });

            // 是否开启按产品内部sku排序,默认正序
            $isSort = $isSort == 'asc' ? SORT_ASC : SORT_DESC;
            $sortItems = [];
            foreach ($items as $item) {
                $sortItems[$item['product_id']][] = $item;
            }

            $newItems = [];
            foreach ($sortItems as $sortItem) {
                $sort = array_column($sortItem, 'sort');
                array_multisort($sort, $isSort, $sortItem);

                array_push($newItems, $sortItem);
            }

            return array_reduce($newItems, function ($result, $value) {
                return array_merge($result, $value);
            }, []);
        });

        // 获取分页信息
        $perPage = post('perPage') ?: 60;
        $page = post('page')?:0;

        // 总产品的sku数量
        $total = count($productOptionValueIdArr);
        $totalPage = (int)ceil($total / $perPage);

        // 获取当前页数据
        $currentPageProduct = array_chunk($productOptionValueIdArr, $perPage, true);
        if (!isset($currentPageProduct[$page])) {
            return $items;
        }
        // 获取当前页的产品ID
        $currentPageProduct = $currentPageProduct[$page];
        $queryProductIdArr = array_keys(array_flip(array_column($currentPageProduct, 'product_id')));
        $products = self::getProductsByIdsFromCache($queryProductIdArr);

        foreach ($currentPageProduct as $item) {
            // optionValueId 选项值ID
            // item 数组包含 选项值ID 和 产品ID

            // 筛选产品
            $product = array_first($products, function($key, $product) use ($item) {
                return data_get($product, 'id') == data_get($item, 'product_id');
            });

            if (!$product) {
                continue;
            }
            // 特征图默认为产品的特征图
            $f_thumb = data_get($product, 'f_thumb');
            // 获取产品的选项
            $options = data_get($product, 'options') ?: [];
            foreach ($options as $option) {
                if ($item['option_id'] != $option['id']) {
                    continue;
                }
                // 筛选出产品选项值对应的图片
                $optionValues = data_get($option, 'values') ?: [];
                foreach ($optionValues as $optionValue) {
                    if ($optionValue['id'] != data_get($item, 'option_value_id')) {
                        continue;
                    }
                    $f_thumb = data_get($optionValue, 'thumb.path');
                }
            }

            // 替换产品特征图
            $product['f_thumb'] = $f_thumb;
            $items[] = $product;
        }

        return compact('perPage', 'page', 'items', 'total', 'totalPage');
    }

    /**
     * 获取前台用户购买此商品的订单数
     * @param $id 商品ID
     * @return bool|int
     */
    public static function userBuyCount($id){

        is_string($id) && $id = (int) $id;

        $userId = (new Account)->user()->id ?? null;

        if (empty($id) || is_null($userId)) {
            return 0;
        }

        $cacheKey = 'userBuyCount-' . $userId;
        $orderData = \Cache::tags("user-buy-count")->remember($cacheKey, 1440, function () use ($userId) {

            $receivedGoodsStatusIds = OrderStatus::saledStatusIds();
            return \DB::table('cc_orders')
                ->join('cc_order_products', 'cc_order_products.order_id', '=', 'cc_orders.id')
                ->where('cc_orders.uid', $userId)
                ->whereIn('cc_orders.status_id', $receivedGoodsStatusIds)
                ->select(DB::raw('sum(qty) as sales, product_id'))
                ->groupBy('cc_order_products.product_id')
                ->lists('sales', 'cc_order_products.product_id');

        });

        return $orderData[$id] ?? 0;

    }

    /**
     * 获取当前用户对$pid评论次数
     * @param $pid 商品ID
     * @return bool|mixed
     */
    public static function reviewCountLog($pid){

        $userId = (new Account)->user()->id ?? null;

        if (empty($pid) || is_null($userId)) {
            return 0;
        }

        $cacheKey = 'user-review' . $userId . '-' . $pid;

        return  \Cache::tags('user-review-count')->remember($cacheKey, 360, function () use ($userId, $pid) {

            return DB::table('cc_reviews')->where(['product_id' => $pid, 'uid' => $userId])->count();

        });

    }

    /**
     * 付款N单可评论N次
     * @param $pid 商品ID
     * @return bool
     */
    public static function isCanReview($pid)
    {
        if (self::userBuyCount($pid) > self::reviewCountLog($pid)) {
            return true;
        }
        return false;
    }
    /**
     * 获取产品积分设置数据 顺序依次为 产品本身积分设置->产品勾选位积分设置->产品勾选位(Algorithms类型,勾选位必须在优先勾选位中)积分设置->产品分类积分设置->全局积分设置
     * @param $product
     * @param int $price
     * @return array
     */
    public function getProductRewardPoint($product,$price = 0)
    {
        $this->product = $product;
        $this->productOriginPrice = $price ? $price : $product->price;
        $this->hasSetPrice = $price ? true : false;//如果有设置金额，则金额是已经经过汇率换算后的，不需要再进行换算
        $currency = Session::get('choice_currency');
        $this->currency = !empty($currency) ? $currency : Currencies::switched();
        $this->getProductSelfBindPoint()
            ->getProductSectionBindPoint()
            ->getProductSectionAlgorithmsBindPoint()
            ->getProductCategoryBindPoint()
            ->getProductGlobalBindPoint();
        return $this->productPointData;

    }

    /**
     * 获取产品本身设置积分策略
     * @return $this
     */
    private function getProductSelfBindPoint()
    {
        if (count($this->product->reward_point) <= 0) {
            return $this;
        }
        foreach ($this->product->reward_point as $point) {
            if ($point->amount_type == 'ratio') {
                if($this->hasSetPrice){
                    $this->productPointData[$point->user_group] = bcdiv(bcmul($this->productOriginPrice, $point->amount),100);
                }else{
                    $this->productPointData[$point->user_group] = bcdiv(bcmul(bcmul($this->productOriginPrice, $this->currency['coefficient']), $point->amount),100);
                }

            } elseif ($point->amount_type == 'specify') {
                $this->productPointData[$point->user_group] = $point->amount;
            }
        }
        return $this;
    }

    /**
     * 获取勾选位Algorithms类型绑定分类积分策略(Algorithms类型勾选位必须在优先勾选位中, 因为勾选位有设置产品数量, 在优先勾选位中产品id直接和缓存的pids做对比)
     * @return $this
     */
    private function getProductSectionAlgorithmsBindPoint(){
        if($this->productPointData){
            return $this;
        }
        $sectionIds = [];
        //是否设置优先勾选位积分设置, 如开启获取设置的勾选位数据缓存
        $sectionArray = self::getFixSectionCache();
        if (!empty($sectionArray)) {
            foreach($sectionArray as $section){
                if(in_array($this->product->id,$section['pids']) && $section['type'] == 'algorithms'){
                    $sectionIds[] = $section['id'];
                    break;
                }
            }
        }
        if(empty($sectionIds)){
            return $this;
        }
        $section_points =  RewardPoint::query()
            ->where('bind_type','Jason\Ccshop\Models\ProductSection')
            ->whereIn('bind_id', $sectionIds)
            ->orderBy('bind_id','desc')
            ->get();

        if($section_points->isEmpty()){
            return $this;
        }
        foreach ($section_points as $item) {
            if(!isset($this->productPointData[$item->user_group]) && ($item->amount > 0)){
                if ($item->amount_type == 'ratio') {
                    if($this->hasSetPrice){
                        $this->productPointData[$item->user_group] = bcdiv(bcmul($this->productOriginPrice,$item->amount),100);
                    }else{
                        $this->productPointData[$item->user_group] = bcdiv(bcmul(bcmul($this->productOriginPrice,$this->currency['coefficient']),$item->amount),100);
                    }
                } elseif ($item->amount_type == 'specify') {
                    $this->productPointData[$item->user_group] = $item->amount;
                }
            }
        }
        return $this;
    }

    /**
     * 获取产品勾选绑定积分策略
     * @return $this
     */
    private function getProductSectionBindPoint()
    {
        if($this->productPointData){
            return $this;
        }
        $sections = ProductSectionSelected::query()
            ->where('product_id', $this->product->id)
            ->orderBy('section_id','desc')
            ->groupBy('section_id')
            ->join('cc_product_sections', 'cc_product_sections.id', '=', 'cc_product_section_selected.section_id')
            ->where('cc_product_sections.is_enabled', '=', 1)
            ->select('cc_product_section_selected.section_id')
            ->get();
        if($sections->isEmpty()){
            return $this;
        }
        $sectionIds = $sections->pluck('section_id')->all();
        //判断是否设置优先勾选位积分设置 设置-商店设置-站点特有
        $fixSectionCode = Settings::fetch('fixSection', null);
        $fixSectionCode && $fixSectionCodeArr = explode(',',$fixSectionCode);

        if (!empty($fixSectionCodeArr)) {
            $sectionPids = ProductSectionService::getSectionPidsByCodes($fixSectionCodeArr);
            foreach($sectionPids as $k=>$pids){
                if(in_array($this->product->id,$pids)){
                    $sectionIds[] = $k;
                    break;
                }
            }
        }
        $section_points =  RewardPoint::query()
            ->where('bind_type','Jason\Ccshop\Models\ProductSection')
            ->whereIn('bind_id', $sectionIds)
            ->orderBy('bind_id','desc')
            ->get();
        if($section_points->isEmpty()){
            return $this;
        }
        foreach ($section_points as $item) {
            if(!isset($this->productPointData[$item->user_group]) && ($item->amount > 0)){
                if ($item->amount_type == 'ratio') {
                    if($this->hasSetPrice){
                        $this->productPointData[$item->user_group] = bcdiv(bcmul($this->productOriginPrice,$item->amount),100);
                    }else{
                        $this->productPointData[$item->user_group] = bcdiv(bcmul(bcmul($this->productOriginPrice,$this->currency['coefficient']),$item->amount),100);
                    }
                } elseif ($item->amount_type == 'specify') {
                    $this->productPointData[$item->user_group] = $item->amount;
                }
            }
        }
        return $this;
    }

    /**
     * 获取产品绑定分类的积分策略
     * @return $this
     */
    private function getProductCategoryBindPoint()
    {
        if($this->productPointData || count($this->product->categories) <= 0){
            return $this;
        }
        foreach ($this->product->categories->reverse() as $category) {
            if (count($category->reward_point) == 0) {
                continue;
            }
            foreach ($category->reward_point as $item) {
                if ($item->amount_type == 'ratio') {
                    if($this->hasSetPrice){
                        $this->productPointData[$item->user_group] = bcdiv(bcmul($this->productOriginPrice,$item->amount),100);
                    }else{
                        $this->productPointData[$item->user_group] = bcdiv(bcmul(bcmul($this->productOriginPrice,$this->currency['coefficient']),$item->amount),100);
                    }
                } elseif ($item->amount_type == 'specify') {
                    $this->productPointData[$item->user_group] = $item->amount;
                }
            }
            return $this;
        }
        return $this;
    }

    /**
     * 获取产品全局积分策略设置
     * @return $this
     */
    private function getProductGlobalBindPoint()
    {
        if($this->productPointData){
            return $this;
        }
        $globalPointSettings = RewardPoint::where([
            'bind_id' => 0,
            'bind_type' => 'Jason\Ccshop\Models\Category',
        ])->get();
        if($globalPointSettings->isEmpty()){
            return $this;
        }
        foreach ($globalPointSettings as $globalPointSetting){
            if ($globalPointSetting->amount_type == 'ratio') {
                if($this->hasSetPrice){
                    $this->productPointData[$globalPointSetting->user_group] =bcdiv(bcmul($this->productOriginPrice,$globalPointSetting->amount),100);
                }else{
                    $this->productPointData[$globalPointSetting->user_group] =bcdiv(bcmul(bcmul($this->productOriginPrice,$this->currency['coefficient']),$globalPointSetting->amount),100);
                }
            } elseif ($globalPointSetting->amount_type == 'specify') {
                $this->productPointData[$globalPointSetting->user_group] = $globalPointSetting->amount;
            }
        }
        return $this;
    }
    /**
     * 获取商品sku价格
     * $flag. 后台sku列表的sku价格项需要一直调取计算出来的价格.所以加个参数.
     */
    public static function getSkuPrice ($sku = '',$product){
        if(empty($product['id'])){
            return 0;
        }
        $price = ShopBase::getDbPrice($product['id']);
        if(empty($sku) || empty($product['sku']) || empty($product['options'])){
            return $price;
        }

        $skuInfo = ProductSku::where('sku',$sku)->where('product_id',$product['id'])->first();
        if (isset($skuInfo->is_enabled_sku_sale_price) && $skuInfo->is_enabled_sku_sale_price == 1) {
            return $skuInfo->sku_sale_price;
        }

        $option_values = [];
        foreach($product['sku'] as $v){
            if($sku == $v['sku']){
                $option_values = json_decode($v['option_values'],true);
                break;
            }
        }
        if(empty($option_values)){
            return $price;
        }
        $skuOptions = [];
        foreach($product['options'] as $option){
            foreach($option['values'] as $op_val){
                if(isset($option_values[$op_val['option_id']][$op_val['id']])){
                    $skuOptions[]=$op_val;
                }
            }

        }

        if(empty($skuOptions)){
            return $price;
        }

        foreach($skuOptions as $op){
            if($op['price_variate'] == '+' && $op['variate_value'] != 0){
                $price += $op['variate_value'];
            }elseif($op['price_variate'] == '-' && $op['variate_value'] != 0){
                $price -= $op['variate_value'];
            }
        }
        if ($price < 0) {
            return 0;
        }
        return $price;
    }


    /**
     * 获取商品sku价格
     * $flag. 后台sku列表的sku价格项需要一直调取计算出来的价格.所以加个参数.
     */
    public static function getSkuPriceWithOutPromotion ($sku = '',$product){
        if(empty($product['id'])){
            return 0;
        }
        $price = $product['src_price'];
        if(empty($sku) || empty($product['skus']) || empty($product['private_options'])){
            return $price;
        }

        $option_values = [];
        foreach($product['skus'] as $v){
            if($sku == $v['sku']){
                $option_values = json_decode($v['option_values'],true);
                break;
            }
        }
        if(empty($option_values)){
            return $price;
        }
        $skuOptions = [];
        foreach($product['private_options'] as $option){
            foreach($option['option_values'] as $op_val){
                if(isset($option_values[$op_val['option_id']][$op_val['id']])){
                    $skuOptions[]=$op_val;
                }
            }

        }
        if(empty($skuOptions)){
            return $price;
        }
        foreach($skuOptions as $op){
            if($op['price_variate'] == '+' && $op['variate_value'] != 0){
                $price += $op['variate_value'];
            }elseif($op['price_variate'] == '-' && $op['variate_value'] != 0){
                $price -= $op['variate_value'];
            }
        }
        if ($price < 0) {
            return 0;
        }
        return $price;
    }


    /**
     * 打版预售图标>获取预售图标
     * @param int $stockPile |Product::getStockPiles
     * @param int $isPresell [0,1]
     * @return array
     * [
     *  'path'=>图标
     *  'description'=>文案
     *  'code'=>图标类型
     * ]
     */
    public static function getPreSaleIcon($stockPile = 0, $isPresell = 0)
    {
        if(is_object($isPresell)){
            $isPresell = $isPresell->toArray();
        }
        $preSalePeriodSwitch = Settings::fetch('is_pre_sale_period');//打版预售开关
        $preSaleSwitch = Settings::fetch('is_pre_sale');//囤货开关
        if ($preSalePeriodSwitch && (int)$isPresell == 1) {
            $fromDate = Settings::fetch('from_date');
            $toDate = Settings::fetch('to_date');
            if (!is_null($fromDate) && !is_null($toDate)) {
                $fromDate = strtotime($fromDate);
                $toDate = strtotime($toDate);
                $nowDate = time();
                if ($nowDate <= $toDate && $nowDate >= $fromDate) {
                    $iconItems = self::getPreSaleIconBannerItems();
                    $icon = $iconItems->where('name', 'pattern_making')->first();
                }
            }
        } elseif ($preSaleSwitch) {
            $stockPiles = Product::getStockPiles();
            if (!empty($stockPile) && in_array($stockPile, $stockPiles)) {
                $iconItems = self::getPreSaleIconBannerItems();
                $bannerTitle = 'suspected';
                switch ($stockPile) {
                    case 1:
                    case 2:
                        $bannerTitle = 'stockpile';
                        break;
                    case 3:
                        $bannerTitle = 'confirm-stock';
                        break;
                }
                $icon = $iconItems->where('name', $bannerTitle)->first();
            }
        }
        return [
            'path' => $icon['image']['path'] ?? '',
            'describe' => $icon['describe'] ?? '',
            'code' => $bannerTitle ?? '',
        ];
    }

    /**
     * 获取预售图标相关
     * @return array| collect
     */
    private static function getPreSaleIconBannerItems()
    {
        if (empty(self::$preSaleIconBannerItems)) {
            $iconBanner = Advertisings::findByCode('pre-sale-icon');
            self::$preSaleIconBannerItems = collect($iconBanner['items'] ?? []);
        }
        return self::$preSaleIconBannerItems;
    }

    /**
     * 获取选中的sku信息
     */
    public static function checkedOption($product_id, $checked_options, $isPresell = 0)
    {

        if (empty($product_id) || empty($checked_options) || !is_array($checked_options)) {
            return [];
        }

        $checked_info = self::getCheckedSkuAndOption($product_id,$checked_options);

        if(empty($checked_info['sku'])){
            return [];
        }

        $checked_sku = $checked_info['sku'];

        $stockLabelId = self::getStocklabelIdBySku($checked_sku);
        $presaleIcon = self::getPreSaleIcon($stockLabelId,$isPresell);

        return [
            'presaleIcon'=>$presaleIcon,
            'sku'=> $checked_sku['sku'],
            'sku_status'=>$checked_sku['sku_status'] ?? 0
        ];
    }
    /**
     * 获取sku囤货标签
     */
    public static function getStocklabelIdBySku($sku = []){
        $stockPiles = Product::getStockPiles();

        $stockpile = $stockPiles['suspected_in_stock'];

        if(empty($sku)){
            return [];
        }

        if(!empty($sku['stockpile_status'])){
            $stockpile = $stockPiles['all_stock_up'];
        }elseif(!empty($sku['store_status'])){
            $stockpile = $stockPiles['confirmed_in_stock'];
        }else{
            $stockpile = $stockPiles['suspected_in_stock'];
        }
        return $stockpile;
    }
    /**
     * 获取用户选中选项的sku以及option信息
     */
    public static function getCheckedSkuAndOption($product_id, $checked_options)
    {
        $product = Products::getProductByCache(['id' => $product_id]);
        $skus = $product['sku'] ?? [];
        $options = $product['options'] ?? [];
        $sku_info = ShoppingCartService::findProductSkuByOptions($product_id,$checked_options,$skus);
        $option_info = self::getCheckedOption($options, $checked_options);
        if (empty($sku_info) && empty($option_info)) {
            $checked_options = array_pluck((array)$checked_options, 'value.0.id', 'value.0.option_id');
            $sku_info = ShoppingCartService::findProductSkuByOptions($product_id,$checked_options,$skus);
            $option_info = self::getCheckedOption($options, $checked_options);
        }
        return ['sku' => $sku_info, 'option' => $option_info];
    }
    /**
     * 或获取选中的选项
     */
    public static function getCheckedOption($options,$checked_options){
        $option_info = [];
        foreach($options as $option){
            foreach($option['values'] as $ov){
                if(isset($checked_options[$ov['option_id']]) && $checked_options[$ov['option_id']] == $ov['id']){
                    $option_info[] = $ov;
                }
            }
        }
        return $option_info;
    }
    /**
     * 获取选中sku
     */
    public static function getSkuByOption($skus,$checked_options){
        $sku_info = [];
        if(empty($checked_options) || !is_array($checked_options)){
            return $sku_info;
        }
        foreach($skus as $sku){
            $op_val = json_decode($sku['option_values'],true);
            $checked = 0;
            foreach($checked_options as $k=>$val){
                if(is_array($val)){
                    continue;
                }
                if(isset($op_val[$k][$val])){
                    $checked ++;
                }
            }
            if($checked == count($op_val)){
                $sku_info = $sku;
                break;
            }
        }
        return $sku_info;
    }

    /**
     * 获取产品SKU选项的积分
     * @param $product product-id | product-(model|array)
     * @return array
     */
    public static function getProductSkuPoints($product)
    {
        if (is_int($product)) {
            $products = self::getProductsByIdsFromCache($product);
            $product = $products[0] ?? [];
        }
        if (empty($product)) {
            return [];
        }
        $cacheKey = Product::getSkuCacheKey($product['id']);
        return Cache::tags('product-sku-switch')->remember($cacheKey, 1440, function () use ($product) {
            $productSkus = $product['sku'] ?? [];
            $skuPoints = [];
            //金币商品操作
            if($product['id'] == GoldCoinSetting::getGoldCoinRelationProductId()) {
                $is_gold_status = 1;
                $goldCoinProduct = GoldCoinService::onLoadGoldCoinProduct();
            }
            foreach ($productSkus as $skuKey => $skuItem) {
                $optionValues = json_decode($skuItem['option_values'], true);
                $optionValueID = [];
                $res = array_walk($optionValues, function (&$item) use (&$optionValueID) {
                    $optionValueID[] = array_keys($item)[0];
                });
                $optionIndex = '';
                $res && !empty($optionValueID) && $optionIndex = implode('_', $optionValueID);
                $skuPrice = self::getSkuPrice($skuItem['sku'], $product);
                $skuPoint = Products::getProductRewardPoints($product['id'], $skuPrice);
                $price_ceil = Settings::fetch('is_down_usd_price_ceil','0');
                $skuPoints[$optionIndex] = [
                    'sku' => $skuItem['sku'],
                    'status' => $skuItem['sku_status'],//1为有货
                    'stockpile_status' => $skuItem['stockpile_status'],
                    'point' => empty($price_ceil) ? (int)$skuPoint : sprintf("%01.2f", $skuPoint) ,
                    'price' => empty($price_ceil) ? (int)$skuPrice : sprintf("%01.2f", $skuPrice) ,
                ];
                //金币商品操作
                if(!empty($is_gold_status) && !empty($goldCoinProduct)){
                    if(!empty($goldCoinProduct[$skuItem['sku']])) {
                        $skuPoints[$optionIndex] = array_merge($skuPoints[$optionIndex], $goldCoinProduct[$skuItem['sku']]);
                    } else {
                        unset($skuPoints[$optionIndex]);
                    }
                }
            }
            return $skuPoints;
        });
    }

    /**
     * 根据code&分类ID&产品ID获取指定商品数据集
     * @description Service层首先根据code值获取当前code对应的所有商品数据
     * @author rsky <renxing@shengyuntong.net>
     * @datetime add 2021年1月21日
     * @param  string   $code               code值
     * @param  array    $cids               分类数组值
     * @param  string   $sort_field         排序方式
     * @param  integer  $day                销量比较的天数，实际调用sortRecentlySaledProductIds方法，默认为15天
     * @return collection->array 数据集
     */
    public static function getCategorySetionProducts(string $code, array $cids, string $sort_field = '', int $day = 0)
    {
        // CODE对应的所有商品
        $productIds = \App::make('Jason\Ccshop\Components\Catalog')->getSectionPids($code);

        $categorySetionProducts = [];

        foreach ($cids as $key => $value) {
            $cid = $value['id'];

            $categorySetionProducts[$cid] = [];

            $cacheKeyArr = [
                $code, $sort_field
            ];

            if ($day > 0) {
                array_push($cacheKeyArr, $day);
            }

            // 缓存Key值 放入code、排序方式、天数数据作区分
            $cacheKey = 'category-section-product' . implode( '-', $cacheKeyArr) . '-' . $cid;

            $categorySetionProducts[$cid] = \Cache::tags('product-sections')->remember($cacheKey, 360, function() use($cid, $productIds, $sort_field, $day)
            {
                // 对应分类ID的所有商品ID
                $sectionCategoryProductIds = ProductCategory::getProductIdByCategoryId($cid, $productIds);

                switch ($sort_field) {
                    // 根据销量排序
                    case 'sales' :
                        if ($day > 0) {
                            $sectionCategoryProductIds = self::sortRecentlySaledProductIds($sectionCategoryProductIds, [], $day);
                        } else {
                            $sectionCategoryProductIds = self::sortRecentlySaledProductIds($sectionCategoryProductIds);
                        }
                        // 上面方法返回['pid' => 销量]，仅获取key值，以便下层使用
                        $sectionCategoryProductIds = array_keys($sectionCategoryProductIds);
                        break;
                    case 'section' :
                        //将分类产品按照勾选位产品id排序
                        $sectionCategoryProductIds = array_values(array_intersect($productIds,$sectionCategoryProductIds));
                        break;
                    default:
                        break;
                }

                if (!empty($sectionCategoryProductIds)) {
                    $sectionCategoryProductIds = self::getProductsByIdsFromCache($sectionCategoryProductIds);
                }

                return $sectionCategoryProductIds;
            });
        }

        return $categorySetionProducts;
    }

    /**
     * 生成不同站点的产品url
     * @param $id
     * @param string $slug
     * @param int $site_id
     * @return string
     */
    public static function generateUrl($id, $slug = '', $site_id = 0)
    {
        if (empty(self::$siteProductUrlFormat)) {
            self::$siteProductUrlFormat = MoresiteService::getSiteUrlFormat($site_id);
        }

        //主域名商品链接不拼接域名ID
        $params = [
            'id' => (int)$id,
            'slug' => $slug,
        ];
        if(!empty($site_id)){
            $siteInfo =current(MoresiteService::getSitesBySiteIds([$site_id]));
            if(!empty($siteInfo['conceal']) && empty($siteInfo['is_main'])){
                $params['id'] += $siteInfo['conceal'];
            }
        }

        return Helper::generateUrl(self::$siteProductUrlFormat, $params);
    }

    public static function formatSceneEsProducts($esProducts, $sceneId)
    {
        foreach ($esProducts as $index => $esProduct) {
            if(isset($esProduct['scene_data'])){
                $sceneData = array_column($esProduct['scene_data'],null,'scene_id');
                if (isset($sceneData[$sceneId])){
                    $esProducts[$index] = array_merge($esProducts[$index],$sceneData[$sceneId]);
                }
            }
        }
        return $esProducts;
    }

    /*
     * 根据产品ID&分类ID获取指定商品数据集
     * @author rsky <renxing@shengyuntong.net>
     * @datetime add 2021年2月3日
     * @param  array    $productIds         产品ID数组
     * @param  array    $cids               分类ID数组
     * @param  array    $keySign            页面区分
     * @param  array    $isSort             是否按照传入的产品ID排序
     * @return collection->array 数据集
     */
    public static function getProductPageByCategoryIDsAndProductIDs(array $productIds, array $cids, string $keySign, $isSort = false)
    {
        // 空数组直接返回
        if (empty($productIds) || empty($cids) || empty($keySign)) {
            return [];
        }

        // 通过配置获取分页条数
        $perPage = Settings::fetch('product_list_display_num_per_page', 60);

        // 缓存Key值 放入分类ID，All分类下表现为1,2,……
        $cacheKey = 'category-product-' . $keySign . implode('-', $cids) . md5(json_encode($productIds));

        // 筛选出商品ID中属于指定分类下的商品分页数据
        $categoryProductIds = \Cache::tags('category-product')->remember($cacheKey, 360, function() use($cids, $productIds, $isSort)
        {
            // 对应分类ID的所有商品ID
            $categoryProductIds = ProductCategory::getProductIdByCategoryIds($productIds, $cids);

            // 取交集解决顺序错乱问题：勾选位商品的ID靠后被排在后面
            $categoryProductIds = array_values(array_intersect($productIds, $categoryProductIds));

            // 将分类下的产品ID按照商品ID顺序排序
            if ($isSort == true) {
                $newCategoryProductIds = [];

                foreach ($productIds as $key => $productId) {
                    // 正常商品排序
                    if (in_array($productId, $categoryProductIds)) {
                        $newCategoryProductIds[] = $productId;
                    }
                }

                $categoryProductIds = $newCategoryProductIds;
            }

            return $categoryProductIds;
        });

        // 根据分类下的商品ID分页
        $products = Helper::manualPaging($categoryProductIds, ['perPage' => $perPage]);

        // 获取数据
        $products['data'] = self::getProductsByIdsFromCache($products['data'] ?? []);

        return $products;
    }

    /**
     * 清理产品路由缓存
     * @param  array  $ids
     */
    public static function clearProductRouteCache(array $ids)
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

        $productRow = array_first($cacheRouteRows, function ($key, $item) {
            return $item['route_pattern'] == '/:slug|[a-z0-9\-]+-p-\d+.html$';
        });

        if (empty($productRow)) {
            return;
        }

        $prefix = Cache::getPrefix();
        $cacheKey = $prefix.'SerenityNow.Cacheroute:';
        $redis = CacheRoute::getRedisInstance();

        $clearKeys = [];
        foreach ($ids as $id) {
            $pattern = $cacheKey.$productRow['id'].':'.$id.':*';
            $allKeysGenerator = RedisService::scanAllKeys($redis, $pattern);

            foreach ($allKeysGenerator as $keys) {
                $clearKeys = array_merge($clearKeys, $keys);
            }
        }

        foreach ($clearKeys as $key) {
            $redis->executeRaw(['UNLINK', $key]);
        }
    }

    /**
     * 热销商品选项颜色添加hot标签
     */
    public static function optionHotLabel($product_id,$opval,$opcount){
        $cacheKey = 'json_ccshop_product_option_hot_sale';
        $sales =  \Cache::tags('products')->remember($cacheKey, 1440,function () use ($cacheKey){
            $sales = Db::table('system_settings')->where('item',$cacheKey)->first();
            if(empty($sales)){
                return [];
            }
            return json_decode($sales->value,true);
        });
        if(!isset($sales[$product_id])){
            return false;
        }

        //颜色等于2种时，给一个热销颜色加“hot”标，当颜色大于等于3种颜色时给销量最高的两个颜色加“hot”标
        $index = array_search($opval,$sales[$product_id]);

        if($index === false){
            return false;
        }

        if(($opcount < 3 && $index == 0) || ($opcount >= 3 && $index <= 1)){
            return true;
        }
        return false;
    }
    /**
     * 根据加购options匹配productSku
     * @param $options
     * @param $productSkus
     * @return bool|mixed
     */
    public static function getMatchSkuByOptions($options,$productSkus)
    {
        $matchOptions = [];
        foreach ($options as $option){
            $optionValue = $option['value'][0];
            $matchOptions[] = intval($option['id'] ?? 0);
            $matchOptions[] = intval($optionValue['id'] ?? 0);
        }
        sort($matchOptions);
        foreach ($productSkus as $productSku){
            $optionValues = json_decode($productSku['option_values'],true);
            if (empty($optionValues)) {
                continue;
            }
            $productSkuOptions = [];
            foreach ($optionValues as $k => $skuValue){
                $productSkuOptions[] = intval($k);
                $productSkuOptions[] = intval(array_keys($skuValue)[0]);
            }
            sort($productSkuOptions);
            if($matchOptions == $productSkuOptions){
                return $productSku;
            }
        }
        return false;
    }

    /**
     * @param $product
     * @param $site
     * @param $scene
     * @return mixed
     * 产品视图数据替换
     */
    public static function sceneSaveProductData($product, $site, $scene)
    {
        if(empty($scene)){
            return $product;
        }
        if (isset($product['scene_data'])) {
            $saveFields = ['name', 'slug', 'feature_image', 'features', 'f_thumb','price','list_price','discount','reviews_count'];
            $dataScene = array_column($product['scene_data'], null, 'scene_id');
            $dataScene = $dataScene[$scene] ?? [];
            if (!empty($dataScene)) {
                foreach ($saveFields as $val) {
                    if (isset($dataScene[$val]) && !empty($dataScene[$val])) {
                        $product[$val] = $dataScene[$val];
                    }
                }
            }
        }
        $slug = $product['slug'] ?? 'product';
        $product['url'] = ProductService::generateUrl($product['id'], $slug , $site);
        return $product;
    }


    /**
     * 根据传入的产品ids获取[start-end]区间的产品数据集
     * @author rsky <longxiqiu@shengyuntong.net>
     * @datetime add 2021年3月7日
     * @param  array  $productIds   产品ids
     * @param  array  $region       获取区间数据（第一个元素是：开始位置，第二个元素：结束位置）
     * @return collection->array    数据集
     */
    public static function getProductRegionData(array $productIds,array $region = [])
    {

        // 空数组直接返回
        if (empty($productIds) || count($region) < 2) {
            return [];
        }
        //开始位置值不能大于结束位置值，否则返回空数据
        if($region[0] > $region[1]){
            return [];
        }

        $limit = $region[1]-$region[0]+1;
        // 获取产品数据第region[0]-region[1]的ID
        $productIds = $region[0] > 0 ? array_slice($productIds, $region[0]-1, $limit) : $productIds;

        // 产品数据
        $productData = self::getProductsByIdsFromCache($productIds);

        return $productData;
    }

    /**
     * 获取经过过滤的产品数据
     * @param $products
     * @param  array  $filterOptions 例子：['price'=>'desc']
     * @param  int  $perPage 每一页的产品数量，false 时为不分页
     *
     * @return array|mixed
     */
    public static function getFilteredProducts($products, array $filterOptions = [], $perPage = 0)
    {
        if(!$products){
            return [];
        }
        if ($perPage === 0){
            $perPage = Settings::fetch('product_list_display_num_per_page', 15);
        }
        $options = [];
        foreach ($filterOptions as $flag => $sort) {
            $option = self::formatSortFilterOption($flag, $sort);
            if ($option){
                $options[] = $option;
            }
        }
        if(!$options){
            if($perPage !== false){
                $products = Helper::manualPaging($products,['perPage' => $perPage]);
            }
            return $products;
        }
        foreach ($options as $sortFlag) {
            $products = self::sortMultiArrayProducts($products, $sortFlag['flag'], $sortFlag['sort']);
        }
        if($perPage !== false){
            $products = Helper::manualPaging($products,['perPage' => $perPage]);
        }
        return $products;
    }

    /**
     * 筛选产品
     * @param  array  $products 产品数组，需要包含筛选的字段
     * @param  array  $cids 分类数组
     * @param  string $priceRange 价格范围，使用~分割，如3000~10000
     *
     * @return array|mixed
     */
    public static function getFilteredProductsByCidsAndPrice($products, $cids = [], $priceRange = "")
    {
        $products = array_filter($products, function($product) use ($priceRange, $cids){
            if($cids && !array_intersect($cids, $product['cids'] ?? [])){
                return false;
            }
            if (!$priceRange){
                return $product;
            }
            list($small,$big) = explode('~',$priceRange);
            if(!$big && $product['price'] >= $small) {
                return $product;
            }elseif($product['price'] >= $small && $product['price'] < $big){
                return $product;
            }
        });
        return $products;
    }

    /**
     * 格式化产品过滤条件
     * @param $filterOptions
     *
     * @return array
     */
    public static function formatSortFilterOption($flag, $sort)
    {
        if($flag == 'newness'){
            $flag = 'instock_time';
        }
        if($sort == 'asc'){
            $sort = SORT_ASC;
        }
        if($sort == 'desc'){
            $sort = SORT_DESC;
        }
        if ($flag && $sort){
            $options = ['flag'=>$flag,'sort'=>$sort];
        }
        return $options;
    }

    /**
     * 产品二维数组根据指定字段排序
     * @param $products
     * @param $flag
     * @param $sort
     *
     * @return mixed
     */
    public static function sortMultiArrayProducts($products, $flag, $sort)
    {
        $sortColumn = array_column($products, $flag);
        if(!$sortColumn){
            return $products;
        }
        $padSize = count($products) - count($sortColumn);
        if($padSize > 0){
            $sortColumn = array_pad($sortColumn, count($sortColumn)+$padSize, null);
        }
        array_multisort($sortColumn, $sort, $products);
        return $products;
    }

    /**
     * 获取按照周销量、新品排序产品数据
     * @param $saledDays
     * @param $newProductDays
     * @param int $saledLimit
     * @param int $newProductLimit
     * @param array $cids
     * @param array $excludeIds
     * @param array $order
     * @param array $featureArr
     * @return mixed
     */
    public static function getRecentlySaledAndNewProductData($saledDays = 7, $saledLimit = 200, $newProductDays = 10,$newProductLimit = 200 ,$cids = [], $excludeIds = [], $order = [],$featureArr=[])
    {
        // 获取销量排序产品ID
        $products = self::getRecentlySaledProductIds($saledDays, $saledLimit, $cids, $excludeIds, $order);

        // 得到周销量所有ID
        $pids = array_keys($products);

        // 获取新品ID数据
        $newPids = ProductService::getNewlyInstockIds($newProductLimit,$cids,$newProductDays);

        // 处理重复的ID
        $pids = array_unique(array_merge($pids,$newPids));

        // 去除不符合特征值的数据
        if(!empty($featureArr)){
            $cacheKey = md5(serialize(['cid'=> $cids,'feature'=>$featureArr]));
            $featureProductIds = \Cache::tags('recently-saled-and-new-products')->remember($cacheKey, 360, function () use ($featureArr) {
                return (new Products)->flushNewCategoryProducts('all', ['instock_time'=>'desc'], false, 0, 0,[],$featureArr);
            });
            $pids = array_intersect($pids,$featureProductIds);
        }

        // 分页获取数据
        $perPage = post('perPage',0);
        if($perPage == 0){
            $perPage = Settings::fetch('product_list_display_num_per_page', 60);
        }

        $pageProducts = Helper::manualPaging($pids, ['perPage' => $perPage]);

        if (!empty($pageProducts['data'])) {
            $pageProducts['data'] = self::getProductsByIdsFromCache($pageProducts['data']);
        } else {
            $pageProducts['data'] = [];
        }

        return $pageProducts;
    }

    /**
     * 排除指定分类下的商品ID
     * @author rsky <renxing@shengyuntong.net>
     * @datetime rsky-add 2021年3月15日
     * @param  array    $productIds 商品ID
     * @param  array    $cids       分类数组值
     * @param  string   $keySign    缓存key的附加信息[hot-sale|m-hot-sale]
     * @return collection->array    数据集
     */
    public static function getProductIdNotInCategoryIds(array $productIds, array $cids, string $keySign = '')
    {
        $keySign = md5(json_encode($productIds));
        $cacheKey = 'not-in-category-product-' . $keySign . implode('-', $cids);

        // 获取数据
        $productIdsByCategoryIds = Cache::tags('category-product')->remember($cacheKey, 60, function() use($productIds, $cids)
        {
            return ProductCategory::getProductIdByCategoryIds($productIds, $cids);
        });

        // 排除掉指定分类中的商品ID
        $diffProductIds = array_diff($productIds, $productIdsByCategoryIds);

        // 允许展示的商品ID
        return array_values($diffProductIds);
    }

    /**
     * 将原来获取分类产品id的方法单独封装
     * @param mixed $category 分类
     * @param  array  $order 排序
     * @param  int  $limit 限制数量
     * @param  int  $subsite 域名id
     * @param  false  $isNew 是否新品排序
     * @param  false  $isStar 是否标星
     * @param  int  $isSelfAndChildrenIds 是否包含子分类
     * @param  array  $region 价格范围
     * @param  array  $features 特增
     * @param  array  $options 选项
     *
     * @return mixed|array
     */

    public static function getCategoryProductIds($category, $order = [], $limit = 0, $subsite = 0, $isNew = false, $isStar = false, $isSelfAndChildrenIds = 0, $region = [], $features = [], array $options = [])
    {
        $cid = null;
        if (is_numeric($category)) {
            $cid = (int)$category;
        } elseif (!empty($category['id'])) {
            $cid = (int)$category['id'];
        }
        if (!$cid){
            return [];
        }
//        $cacheKey = (new Categories)->getDefaultCachekey($cid, $subsite, $isNew, $order, $limit);
        $cacheKey = Categories::getDefaultCachekey($cid, $subsite, $isNew, $order, $limit);
        return \Cache::tags(['category-products', 'category-products-' . $cid])->remember($cacheKey, 360, function () use ($category, $order, $isStar, $isSelfAndChildrenIds, $limit ,$region, $features, $isNew, $subsite, $options) {
            return (new Products())->flushCategoryProducts($category, $order, $isStar, $isSelfAndChildrenIds, $limit ,$region, $features, $isNew, $subsite, $options);
        });
    }

    /* 对pids进行cids分组,保持传入pids排序
     * @param array $pids
     * @param array $cids
     * @param string $keySign
     * @return mixed
     */
    public static function getPidsGroupCache(array $pids, array $cids, string $keySign = '')
    {
        $cacheKey = 'pids-group-cids-' . $keySign . md5(json_encode($pids) . json_encode($cids));
        return Cache::tags('category-products')->remember($cacheKey, 360, function () use ($pids, $cids) {
            $pidsToCidsData = ProductCategory::select(['product_id', 'category_id'])->whereIn('product_id', $pids)->whereIn('category_id', $cids)->get();
            $pidsToCidsData = $pidsToCidsData->groupBy('category_id')->toArray();
            $pidsToCidsGroup = [];
            foreach ($cids as $cid) {
                $cidPids = array_pluck($pidsToCidsData[$cid] ?? [], 'product_id');
                $pidsToCidsGroup[$cid] = array_values(array_intersect($pids, $cidPids));
            }
            return $pidsToCidsGroup;
        });
    }

    /**
     * 二维数组分段切除组合新数组
     * 备注：将二维数组pids【$catePids】,按【$numberSlices】数量进行分段切除组合成新数组
     * getPidsGroupCache(),[getCategoryProductIdsByPromotionCode]都可以进行后续处理;
     * @param $catePids
     *  $catePids = [
     *      key1 =>[pid11, pid12, pid13.....],
     *      key2 =>[pid21, pid22, pid23.....],
     *      key3 =>[pid34, pid32, pid33.....],
     *      ...qr
     *  ]
     * @param int $numSlices 循环切除默认数量
     *  $numSlices = 2
     *  return [pid11, pid12, pid21, pid22, pid34, pid32, pid13, pid23, pid33...]
     * @param array $cateNumSlices 更细致的控制每个pids数组的循环切除数量,
     *  $cateNumSlices = [key1 => 2, key2 => 3, key3 => 2]
     *  return [pid11, pid12, pid21, pid22, pid23, pid34, pid32, pid13, pid33...]
     * @return array
     */
    public static function compilePidsByLoopSlice($catePids, $numSlices = 0, $cateNumSlices = [])
    {
        if (get('debug_test')) {
            $startTime = microtime(true);
        }
        if ($numSlices == 0) {
            $flattenCatePids = array_flatten($catePids);
            return array_unique($flattenCatePids);
        }
        $arr = [];
        $maxSize = 0;
        foreach ($catePids as $key => $pids) {
            if(empty($pids)){
                unset($catePids[$key]);
                continue;
            }
            $numSlice = (int)($cateNumSlices[$key] ?? $numSlices);
            $arr[$key] = $numSlice > 0 ? array_chunk($pids, $numSlice) : [];
            $maxSize = max($maxSize, count($arr[$key]));
        }

        $items = [];
        while($maxSize) {
            foreach ($arr as $cid => &$pids) {
                $ids = array_shift($pids);

                if (!empty($ids)) {
                    $items = array_merge($items, $ids);
                }
            }
            $maxSize--;
        }
        if (get('debug_test') && !empty($startTime)) {
            $totalTime = (microtime(true) - $startTime) * 1000;
            info(sprintf("总耗时: %s\n 系统负载: %s", $totalTime, json_encode(sys_getloadavg())));
        }

        return array_values(array_unique($items));
    }

    /**
     * 获取多个指定类目的指定数量产品
     * @param array $cids 分类数组 [1,2,3,4]
     * @param array $arrLimit 每个分类返回几条数据 [3=>7,2=>7,4=>7,5=>7,6=>7,7=>7,50=>7]
     * $limit int  sql里每个分类取多少条数据
     */
    public static function getCatesProducts($cids = [], $arrLimit=[], $limit=20){
        $cacheKey = 'cates-group-cids-pro'. md5(json_encode($cids));
        return Cache::tags('category-products')->remember($cacheKey, 180, function () use ($cids, $arrLimit, $limit) {
            $cidStr = implode(',', $cids);
            /**
             * sql说明：
            @num作为记录行号用，@ba这个作为记录分类值的  当@ba的值等余category_id的值时，
            @num的值加1, 当不等于的时候设为1,行号小于等于7 就表示每个分类只取7行数据，就是这里： t3.nu<=:nu
            出来的结果是这样：
            分类   商品id
            分类1   5
            分类1   4
            分类2   7
            分类2   9
            商品id是通过sort跟category_id排好序的
             */
            $result = DB::select("select t3.product_id,t3.category_id from (select t1.product_id,t1.category_id,if(@ba=t1.category_id,@num:=@num+1,@num:=1) as nu,@ba:=t1.category_id from (select product_id,category_id from cc_product_categories where category_id in ({$cidStr})
order by category_id,sort) t1,(select @num:=0,@ba:='') t2) t3 where t3.nu<=:nu",[':nu'=>$limit]);
            if (empty($result))
                return [];
            $ids = [];

            $tmpCatPids = [];
            foreach ($result as $k =>$obj ) {
                $ids[] = $obj->product_id;
                $tmpCatPids[$obj->category_id][] = $obj->product_id;
            }

            $ids = array_values(array_unique($ids));
            $data = self::getProductsByIdsFromCache($ids, ['instock','stocktension']);
            $tmpArr = [];
            $tmpArr = array_column($data, null, 'id');
            $resultArr = [];
            foreach ($cids as $k => $cid ) {
                $resultArr[$cid] = [];
            }

            foreach ($tmpCatPids as $cid => $products) {
                foreach ($products as $key => $pid) {
                    if (count($resultArr[$cid]) < $arrLimit[$cid]) {
                        if (isset($tmpArr[$pid])) {
                            $resultArr[$cid][] = $tmpArr[$pid];
                        }
                    }
                }
            }
            return $resultArr;
        });
    }


    /**
     * [获取满足条件价格区间对应的 产品价格区间产品ids]
     * @param int   $day              天数
     * @param int   $conditionPrice   条件价格
     * @param array $area             条件价格区间 格式：['2499-2799','1999-2499']
     * @param array $priceArea        筛选价格区间 格式：['2799-3099','2499-2799']
     * @param int   $count            每个价格区间显示产品数量
     * @return array
     */
    public static function getConditionPriceAreaProducts( int $day, int $conditionPrice, array $area = [], array $priceArea = [], int $count = 12)
    {

        if(empty($conditionPrice)){
            return [];
        }
        //获取销量产品ids
        $salePids = array_keys(self::getRecentlySaledProductIds($day));

        $saleProducts = [];
        $conditionPriceArea = '';
        foreach($area as $key => $value){
            $areaArr = explode('-',$value);
            if(count($areaArr) < 2){
                continue;
            }
            if($conditionPrice > $areaArr[0] && $conditionPrice <= $areaArr[1]){

                if(!isset($priceArea[$key])){
                    return [];
                }
                $conditionPriceArea = $priceArea[$key];
                break;
            }

        }

        //获取满足条件价格区间的产品数据
        $saleProducts = self::filterProductsByPriceArea($salePids,$conditionPriceArea,$count);

        return $saleProducts;
    }

    /**
     * [获取不同条件价格区间对应的产品价格区间产品id]
     * @return array  $pids         产品ids
     * @return string  $priceArea   筛选价格区间  格式： '2799-3099'
     * @return int    $limit        产品数量
     * @return array  结果集         结构格式： [ids]
     */
    public static function filterProductsByPriceArea(array $pids,string $priceArea, int $limit)
    {
        $areaProducts = [];

        $cacheKey = md5('area-price-products-'.json_encode($pids).$priceArea.$limit);
        //获取满足价格区间的产品ids
        $conditionPids = Cache::tags('category-products')->remember($cacheKey,180, function () use ($pids, $priceArea, $limit){

            $conditionPids = [];    //存储满足条件价格区间产品

            $priceArr = explode('-',$priceArea);

            if(count($priceArr) < 2){
                return [];
            }
            if(empty($pids)){
                return [];
            }
            //通过查库获取产品ids
            $conditionPids = DB::table('cc_products')
                ->whereIn('id',$pids)
                ->whereBetween('price',[$priceArr[0],$priceArr[1]])
                ->limit($limit)
                ->lists('id');

            return $conditionPids;
        });

        $areaProducts = self::getProductsByIdsFromCache($conditionPids);

        return $areaProducts;
    }

    /**
     * 筛选勾选位code商品 [不同价格区间, 不同排序, 不同分类] [仅限手动勾选使用]
     * - 未处理勾选位排序相关
     *
     * @param string $code      勾选位code
     * @param string $priceArea 筛选价格区间  格式： '2799-3099'
     * @param string $options   筛选排序  格式： 'price_desc'
     * @param array  $cids      分类id
     * @param int    $limit     产品数量
     * @param int    $perPage   分页条数
     *
     * @return array  结果集     结构格式：分页数据
     */
    public static function filterProductsBySectionCode(string $code,string $priceArea = '', string $options = '', array $cids = [],  int $limit = 0, int $perPage = 0)
    {
        $cacheKey = md5(json_encode(func_get_args()));
        //获取满足价格区间的产品ids
        $conditionPids = Cache::tags(['price-option-products', 'price-option-products-' . $code])->remember($cacheKey,180, function () use ($code, $priceArea, $options, $cids, $limit){
            if(empty($code)){
                return [];
            }

            //通过查库获取产品ids
            $query = DB::table('cc_product_sections as ps')
                ->leftJoin('cc_product_section_selected as pss', 'ps.id', '=', 'pss.section_id')
                ->leftJoin('cc_products as p', 'p.id', '=', 'pss.product_id')
                ->where('ps.code', $code)
                ->where('ps.type', 'select')
                ->where('p.status', 'instock')
                ->where('ps.is_enabled', 1);

            //判断是否有分类筛选
            if (!empty($cids) && is_array($cids)) {
                $query->leftJoin('cc_product_categories as c', 'c.product_id', '=', 'p.id')->whereIn('c.category_id', $cids);
            }

            //判断是否有价格筛选
            $priceArr = explode('-',$priceArea);
            if(!empty($priceArea) && count($priceArr) == 2){
                $query->whereBetween('p.price',[$priceArr[0],$priceArr[1]]);
            }

            //判断是否筛选排序
            $sortArr = explode('_', $options);
            if (!empty($sortArr) && count($sortArr) == 2) {
                $filed = $sortArr[0];
                $sort = $sortArr[1];
                if ($filed == 'newness') {
                    $filed = 'instock_time';
                }
                $query->orderBy("p.$filed", "$sort");
            } else {
                $query->orderBy('pss.sort', 'asc');
            }

            if ($limit) {
                $query->limit($limit);
            }

            $conditionPids = $query->lists('p.id');

            return $conditionPids;
        });

        return Helper::manualPaging($conditionPids, ['perPage' => $perPage]);
    }



    /**
     * 显示这个产品的二级分类，没有二级分类就往上取一级分类
     * 包装产品分类name
     * @param array $products 所有产品数据
     * @return mixed 产品数据添加分类name
     */
    private static function packageProductsCategoryName($products = []){
        $openPromotionAndProductSectionAddCategoryNameInfo = Settings::fetch('open_promotion_and_product_section_add_category_name_info', false);
        if(!$openPromotionAndProductSectionAddCategoryNameInfo || empty($products)){
            return $products;
        }
        $categoriesTree = current((new Category)->getCategoriesTree());
        //一级分类
        $categoriesMainName = [];
        //二级分类
        $categoriesLevelName = [];
        foreach($categoriesTree['children'] as $key => $category){
            $categoriesMainName[$category['id']] = $category['name'];
            if(!empty($category['children'])){
                foreach($category['children'] as $k => $cate){
                    $categoriesLevelName[$cate['id']] = $cate['name'];
                }
            }
        }

        $categoriesNameMainIds = array_keys($categoriesMainName);
        sort($categoriesNameMainIds);
        $categoriesNameLevelIds = array_keys($categoriesLevelName);
        sort($categoriesNameLevelIds);

        foreach($products as &$product){
            if(empty($product['cids'])){
                continue;
            }
            $categoriesLevelIds = array_intersect($product['cids'], $categoriesNameLevelIds);
            if(!empty($categoriesLevelIds)){
                $product['category_name'] = $categoriesLevelName[current($categoriesLevelIds)];
            }

            if(isset($product['category_name'])){
                continue;
            }
            $categoriesMainIds = array_intersect($product['cids'], $categoriesNameMainIds);
            if(!empty($categoriesMainIds)){
                $product['category_name'] = $categoriesMainName[current($categoriesMainIds)];
            }
        }
        return $products;
    }


    /**
     * 显示购物车产品的二级分类，没有二级分类就往上取一级分类
     * 包装购物车产品分类name
     * @param array $items 购物车产品数据
     * @return array|mixed
     */
    public static function packageShoppingCartProductsCategoryName($items = []){
        $openPromotionAndProductSectionAddCategoryNameInfo = Settings::fetch('open_promotion_and_product_section_add_category_name_info', false);
        if(!$openPromotionAndProductSectionAddCategoryNameInfo || empty($items)){
            return $items;
        }

        $newData = [];
        $productIds = $items->pluck('product_id')->all();

        if(empty($productIds)){
            return $items;
        }

        $productIds = array_unique($productIds);
        $categories = DB::table('cc_categories')->select(DB::raw('cc_categories.id as category_id, cc_categories.nest_left, cc_categories.name ,cc_product_categories.product_id as id'))
            ->join('cc_product_categories', 'cc_categories.id', '=', 'cc_product_categories.category_id')
            ->whereIn('cc_product_categories.product_id', $productIds)
            ->orderBy('cc_categories.nest_left', 'asc')
            ->get();

        foreach ($categories as $category){
            $category = json_decode(json_encode($category), true);

            if(isset($newData[$category['id']])){
                $newData[$category['id']]['cids'][] = $category['category_id'];
            }else{
                $category['cids'][] = $category['category_id'];
                $newData[$category['id']] = $category;
            }
        }

        $products = self::packageProductsCategoryName($newData);
        foreach ($items as &$item){
            if(empty($products[$item->product_id])){
                continue;
            }
            $item->product->cids = $products[$item->product_id]['cids']??[];
            $item->product->category_name = $products[$item->product_id]['category_name']??'';
        }
        return $items;
    }

    /**
     * @description: 获取商品ID的最小捆绑价格
     * @param {int} $pid
     * @return {*}
     */
    public static function getBindMinPrice(int $pid)
    {
        $bundleSales = static::$promotionBindInfo;
        if (empty($bundleSales)) {
            $bundleSales = static::$promotionBindInfo = PromotionService::getCartAndBundlePromotion();
        }

        $key = 'bind_min_price_'.$pid;

        return Cache::tags('products.price')->remember($key, 180, function () use ($bundleSales,$pid) {

            $minPrice = 0;
            foreach($bundleSales as $bundleItem) {
                if(empty($bundleItem['conditions'])){
                    continue;
                }

                $bundleProductId = 0;
                $newBundleItem = collect($bundleItem['conditions'])->groupBy('type')->toArray();
                if(!isset($newBundleItem['products']) || !isset($newBundleItem['order_items'])){
                    continue;
                }

                $productConditions = $newBundleItem['products'][0] ?? [];
                if($productConditions){
                    $checkRes = PromotionService::checkPidInCondition($productConditions,$pid);
                    $bundleProductId = $checkRes ? $pid : $bundleProductId;
                }

                if(!$bundleProductId){
                    continue;
                }

                foreach($newBundleItem['order_items'] as $condition){
                    $num = $condition['values'][0] ?? 0;
                    if(!$num){
                        continue;
                    }

                    if($minPrice){
                        $minPrice = ($condition['bundle_price'] > $minPrice) ?  $minPrice : $condition['bundle_price'] ;
                    }else{
                        $minPrice = $condition['bundle_price'];
                    }
                }

                if($minPrice){
                    break;
                }
            }

            return $minPrice;
        });
    }

    /**
     * 获取单个商品真实销量
     * @param   int     $pid 商品id
     * @param   string  $type 下单 placed 销量 saled
     * @param   int     $day  天数 0 3 7 15 30
     *                  对应 Jason\Ccshop\Console\BuildProductOrderStats 方法中的天数
     * @return  int     返回指定商品的真实销量数
     */
    public static function getProductRealSale( int $pid, int $day=30, string $type='saled' ){
        $sales = self::getProductRealSales( [$pid], $day, $type );
        return $sales[$pid] ?? 0;
    }

    public static function checkRealSalesRedisIsset($type)
    {
        //hash 域名称
        $prefix = Cache::getPrefix();
        $hashAreaName = sprintf( '%shash:products-real-sale-%s', $prefix , $type );

        //获取db1的连接
        $redis = RedisService::getRedisStoreClient();

        return $redis->EXISTS( $hashAreaName);
    }
    /**
     * 获取多个商品真实销量
     * @param   array   $pid 商品id组
     * @param   string  $type 下单 placed 销量 saled
     * @param   int     $day  天数 0 3 7 15 30
     *                  对应 Jason\Ccshop\Console\BuildProductOrderStats 方法中的天数
     * @return  array   [pid1=>sale,pid2=>sale....] 返回对应数组  商品id为key 销量为值
     */
    public static function getProductRealSales( array $pids, int $day=30, string $type='saled' )
    {
        //hash 域名称
        $prefix = Cache::getPrefix();
        $hashAreaName = sprintf( '%shash:products-real-sale-%s', $prefix , $type );

        //获取db1的连接
        $redis = RedisService::getRedisStoreClient();

        //获取销量
        $sales = $redis->hmget( $hashAreaName, $pids );

        foreach( $sales as $key => $sale ){
            if( is_null( $sale ) ){
                $sales[$key] = 0;
            }else{
                $sale = json_decode( $sale, true);

                $sales[$key] = $sale[$day] ?? 0;
            }
        }

        //组合pids和销量
        $productSaled = array_combine( $pids, $sales );

        return $productSaled;
    }

    /**
     * 通过给定的ID集合和状态, 筛选合格的产品ID
     * @param array $productIds
     * @param array $statuses .e.g: ['instock', ...]
     * @return array  if product-status hash cache is not exist, return the first parameter; else return filtered data
     */
    public static function filterProductIdByStatus(array $productIds, array $statuses = [])
    {
        if(empty($productIds)){
            return [];
        }

        empty($statuses) && $statuses = static::$instockStatus;

        $cacheKey = self::getProductStatusCacheKey();
        $redis = RedisService::getRedisStoreClient();
        if (!$redis->exists($cacheKey)) {
            return $productIds;
        }
        $items = $redis->hmget($cacheKey, $productIds);

        $validedPids = [];
        foreach ($items as $key => $item) {
            if (empty($item) || !in_array($item, $statuses) || !isset($productIds[$key])) {
                continue;
            }
            $validedPids[] = (int)$productIds[$key];
        }

        return $validedPids;
    }

    /**
     * 判断产品是否存在
     * @param string $productId
     *
     * @return int
     */
    public static function checkExistByProductId($productId)
    {
        return Db::table('cc_products')->where('id', $productId)->count();
    }

    /**
     * 是否设置优先勾选位积分设置, 如开启获取设置的勾选位数据
     * @param false $refreshCache 为true直接更新缓存
     * @return array|mixed
     */
    public static function getFixSectionCache($refreshCache = false)
    {
        //判断是否设置优先勾选位积分设置 设置-商店设置-站点特有
        $fixSectionCode = Settings::fetch('fixSection', null);
        if (empty($fixSectionCode)) {
            return [];
        }
        $fixSectionCodeArr = explode(',', $fixSectionCode);
        $sectionArray = [];
        if(!$refreshCache){
            $sectionArray = \Cache::get('fix-section-product-section-' . json_encode($fixSectionCodeArr), []);
        }
        if(!empty($sectionArray)){
            return json_decode($sectionArray, true);
        }
        // 先清除getByCodes预加载categorys缓存
        Cache::tags('sysn-product-section-categories')->flush();
        $sectionArray = ProductSection::getByCodes($fixSectionCodeArr);
        foreach ($sectionArray as &$section) {
            if ($section->type == 'newest') {
                $productsNewest = $section->productsectionselecteds->sortBy('sort');
                $productsNewestPids = [];
                foreach ($productsNewest as $key => $productNewest) {
                    array_push($productsNewestPids, $productNewest->product_id);
                }
                $section->pids = $productsNewestPids;
            } else {
                $section->pids = $section->getItems();
            }
        }
        $sectionArray = $sectionArray->toArray() ?? [];
        \Cache::put('fix-section-product-section-' . json_encode($fixSectionCodeArr), json_encode($sectionArray), 1440);
        return $sectionArray;
    }
    /**
     * @description: 将上架商品信息推送到产品库
     * @param {*} $spu
     * @param {*} $productId
     * @return {*}
     */
    public static function productStatusChangeJob($spu, $productId)
    {
        try {

            if (empty($spu)) {
                throw new Exception('SPU 不能为空！');

                return false;
            }

            $job = ProductsStatusChangeJob::create([
                'status' => 'pending',
                'spu' => $spu ?? '',
                'error_msg' => '',
                'time' => '',
                'product_id' => $productId,
                'user_email' => BackendAuth::getUser()->email ?? '',
                'user_id' => BackendAuth::getUser()->id ?? 0,
                'return_msg' => '',
            ]);

        } catch (Exception $e) {
            info('队列异常' . PHP_EOL . $e->getMessage() . PHP_EOL . $e->getTraceAsString());

            return false;
        }

        // 推向队列
        Queue::push(ProductStatusChangeJob::class, $job->id);
    }

    /**
     * 保存产品,验证勾选同步更新积分和优先勾选位开启,清理优先勾选位产品积分缓存
     * @param $productId
     */
    public static function clearFixSectionAndPointCache($productId){
        $openFixSectionSyncUpdate = Settings::fetch('openFixSectionSyncUpdate', '');
        if(!$openFixSectionSyncUpdate){
            return;
        }
        //这里修改了分类,勾选位勾选了对应分类的勾选位产品应该更新, 所以获取最新的勾选位产品,产品和优先勾选位中的ids对比
        $sectionArray = self::getFixSectionCache(true);
        $refreshCache = false;
        foreach ($sectionArray as $value) {
            if (in_array($productId, $value['pids'])) {
                $refreshCache = true;
                break;
            }
        }
        if (!$refreshCache) {
            return;
        }
        //清除积分产品相关缓存
        $userGroups = DB::table('user_groups')->orderby('id')->lists('id');
        $price = ShopBase::getDbPrice($productId);

        Cache::tags('product-point')->forget($productId);
        Cache::tags(['products', 'product-reward-point'])->forget($productId.'-');
        Cache::tags(['products', 'product-reward-point'])->forget($productId.'-'.$price);

        foreach ($userGroups as $userGroup){
            $userGroup = [$userGroup];
            $productCache = $productId.':'.md5(json_encode($userGroup));
            $pointCache = $productId.'-'.$userGroup[0].'-'.$price;
            Cache::tags('product-point')->forget($productCache);
            Cache::tags(['products', 'product-reward-point'])->forget($pointCache);
        }
    }

    /**
     * Notes: 获取销量排行榜product_id
     * @param $number // 需要补充ID数量
     * @return mixed
     */
    public static function getTopSales($number, $sectionPids)
    {
        $key = 'joint-products';
        $tags = "get-top-sales:";
        $startTime = date('Y-m-d', strtotime("- 15days"));
        $endTime = date('Y-m-d', time());
        if ($number > 0 && $number < 18){
            // 销量排行榜前15天 补充18个  不包含当天
            // 缓存
            $cacheResult = \Cache::tags($tags)->remember($key, 60, function() use ($startTime, $number, $endTime, $sectionPids) {
                // 拿到最近销量排行榜
                $tmp = Order::select(DB::raw("id"))
                    ->where("cc_orders.created_at", ">", $startTime)
                    ->where("cc_orders.created_at", "<", $endTime);
                $result = \DB::table(\DB::raw("({$tmp->toSql()}) as temp"))
                    ->mergeBindings($tmp->getQuery())
                    ->selectRaw("cc_order_products.product_id,
             sum( cc_order_products.qty ) as sqty")
                    ->join("cc_order_products", "cc_order_products.order_id", "=", "temp.id")
                    ->groupBy("cc_order_products.product_id")
                    ->orderBy("sqty", "DESC")
                    ->limit(30)
                    ->get();
                $productIds = [];
                if (!empty($result)){
                    // 随机打乱数组
                    shuffle($result);
                    // 补充至number数
                    foreach($result as $key => $res){
                        // 判断拿到的product_id是否已经存在　
                        if (!in_array($res->product_id, $sectionPids)){
                            $productIds[] = $res->product_id;
                        }
                        // 补足则退出循环
                        if(count($productIds) > $number){
                            break;
                        }
                    }
                }
                return $productIds;
            });
            return $cacheResult;
        }else{
            //调用销量榜近7天前30的产品，随机选择18个进行展示
            $startTime = date('Y-m-d', strtotime("- 7days"));
            $cacheResult = \Cache::tags($tags)->remember($key, 60, function() use ($startTime, $number, $endTime) {
                // 拿到最近销量排行榜
                $tmp = Order::select(DB::raw("id"))
                    ->where("cc_orders.created_at", ">", $startTime)
                    ->where("cc_orders.created_at", "<", $endTime);
                $result = \DB::table(\DB::raw("({$tmp->toSql()}) as temp"))
                    ->mergeBindings($tmp->getQuery())
                    ->selectRaw("cc_order_products.product_id,
             sum( cc_order_products.qty ) as sqty")
                    ->join("cc_order_products", "cc_order_products.order_id", "=", "temp.id")
                    ->groupBy("cc_order_products.product_id")
                    ->orderBy("sqty", "DESC")
                    ->limit(30)
                    ->get();
                $productIds = [];
                if (!empty($result)){
                    // 随机打乱数组
                    shuffle($result);
                    // 补充至number数
                    foreach($result as $key => $res){
                        if($key < 18){
                            $productIds[] = $res->product_id;
                        }
                    }
                }
                return $productIds;
            });
            return $cacheResult;
        }
    }

    /**
     * Notes: 获取X天 销量排行Y个产品ID
     * @param $day
     * @param $number
     * @return mixed
     */
    public static function getSaleProductIdByDay($day=3, $number=10){
        $key = "products:$day:$number";
        $tags = "getSalesProductIdByDay:";
        $startTime = date('Y-m-d', strtotime("- {$day}days"));
        $endTime = date('Y-m-d', time());
        // 第二天凌晨时间戳
        $lastDayTimes = strtotime(date('Y-m-d 00:00:00', strtotime("+ 1days")));
        // 当天剩余分钟
        $remainingTime =  ceil(($lastDayTimes - time())/60);
        return \Cache::tags($tags)->remember($key, (int)$remainingTime, function() use ($startTime, $endTime, $number) {
            $orderStatus = OrderStatus::select(["id", "code"])->whereIn("code", ["paid", "delivered", "confirmreceipt"])->lists("id");

            $tmp = Order::select(DB::raw("id"))
                ->where("cc_orders.created_at", ">", $startTime)
                ->where("cc_orders.created_at", "<", $endTime)
                ->whereIn("cc_orders.status_id", $orderStatus);
            $result = \DB::table(\DB::raw("({$tmp->toSql()}) as temp"))
                ->mergeBindings($tmp->getQuery())
                ->selectRaw("cc_order_products.product_id,
         sum( cc_order_products.qty ) as sqty")
                ->join("cc_order_products", "cc_order_products.order_id", "=", "temp.id")
                ->groupBy("cc_order_products.product_id")
                ->orderBy("sqty", "DESC")
                ->limit($number)
                ->lists("product_id");

            return $result;

        });
    }


    /**
     * Notes: 获取24小时销量排行随机取6个
     * @param int $number
     * @return array|null
     */
    public static function getTopSaleByHours($number=6): array
    {
        $key = 'shopping-cart-product-sales-top';
        $tags = "get-top-sales:20";
        $startTime = date('Y-m-d', strtotime("- 365days"));
        $endTime = date('Y-m-d', time());
        $hTime = date('H', time());
        $mTime = date('i', time());
        $relTime = 1440 - ($hTime * 60 + $mTime);
        $cacheResult = \Cache::tags($tags)->remember($key, $relTime, function() use ($startTime, $number, $endTime) {
            // 拿到最近销量排行榜
            $tmp = Order::select(DB::raw("id"))
                ->where("cc_orders.created_at", ">", $startTime)
                ->where("cc_orders.created_at", "<", $endTime);
            $result = \DB::table(\DB::raw("({$tmp->toSql()}) as temp"))
                ->mergeBindings($tmp->getQuery())
                ->selectRaw("cc_order_products.product_id,
             sum( cc_order_products.qty ) as sqty")
                ->join("cc_order_products", "cc_order_products.order_id", "=", "temp.id")
                ->groupBy("cc_order_products.product_id")
                ->orderBy("sqty", "DESC")
                ->limit(20)
                ->get();
            $productIds = [];
            if (!empty($result)){
                // 随机打乱数组
                shuffle($result);
                // 补充至number数
                foreach($result as $key => $res){
                    if($key < $number){
                        $productIds[] = $res->product_id;
                    }
                }
            }
            return $productIds;
        });
        if (empty($cacheResult)){
            return [];
        }
        return ProductService::getProductsByIdsFromCache($cacheResult);
    }

    /**
     * 向redis中写入需要重新计算促销价格的产品
     * @param  array  $pids
     * @return void
     */
    public static function putReCalculatePromotionPriceProducts(array $pids)
    {
        if (empty($pids)) {
            return;
        }

        $prefix = Cache::getPrefix();
        $redis = RedisService::getRedisStoreClient();

        $key = $prefix."waiting-calculate-promotion-price-products";

        $redis->sadd($key, ...$pids);
    }

    /**
     * 更新商品tag数量字段
     */
    public static function updateTagsCount($pids){
        if(empty($pids) || !is_array($pids)){
            return true;
        }

        $data = DB::table('cc_products_to_tags as pt')
            ->join('cc_tags as t','t.id','=','pt.tag_id')
            ->where('t.type',4)
            ->select('product_id',DB::raw('count(product_id) as tags_count'))
            ->whereIn('product_id',$pids)
            ->groupBy('product_id')
            ->get();

        if(empty($data)){
            return true;
        }
        $res = [];
        foreach ( $data as $value ){
            $res[] = [
                'id' => $value->product_id,
                'tags_count' => $value->tags_count
            ];
        }
        Helper::updateBatch('cc_products', $res);
        return true;
    }

    /**
     * 更新tag表产品数量
     * @param string $name
     * @return bool
     */
    public static function updateTagProductCount( $tag_id )
    {
        if( !$tag_id || !is_numeric($tag_id) ) return true;
        $qty = DB::table('cc_products_to_tags')->where('tag_id',$tag_id)->count();
        Tag::where('id',$tag_id)->update(['qty'=>$qty]);
        return true;
    }
    /**
     * 更新商品关联数量字段
     */
    public static function updateRelatedCount($pid){
        if(empty($pid)){
            return true;
        }
        $count = DB::table('cc_product_related_to_products')->where('related_id',$pid)->count();
        DB::table('cc_products')->where('id',$pid)->update(['related_count'=>$count]);
        return true;
    }

    public static function updateChainTotal($pid)
    {
        if(empty($pid)){
            return true;
        }
        DB::table('cc_products')->where('id',$pid)->update(['chain_total'=>0]);
        return true;
    }

    /**
     * 获取产品信息（可同时获取多个数据）
     *
     * @param \Request $request
     * @return array
     */
    public static function getProductInfoByIds(\Request $request)
    {
        $pid = post('pid', []);
        if (is_array($pid)) {
            $pids = array_filter($pid, function ($pidItem) {
                return is_numeric($pidItem);
            });
        } else {
            $pids = [(int)$pid];
        }

        if (empty($pids)) {
            return [
                'code' => 101,
                'msg' => '传入的参数错误！',
                'data' => []
            ];
        }

        // 验证域名是否正确
        $domain = $request::url();
        $domains = MoresiteService::getMainSiteList();
        $domainList = [];
        $siteId = 0;

        $domainArr = parse_url($domain);
        $domain = $domainArr['host'] ?? '';
        if(Str::startsWith($domain, 'www.')){
            $domain = substr($domain, 4);
        }

        foreach ($domains as $key => $domainItem) {
            $domainStr = str_replace('www.', '', $key);
            $domainList[$domainItem['id']] = $domainStr;
            if ($domain == $domainStr) {
                $siteId = $domainItem['id'];
            }
        }

        if (!in_array($domain, $domainList)) {
            info('获取产品数据接口日志domain:' . $domain);
            return [
                'code' => 102,
                'msg' => '传入的参数(域名)错误！',
                'data' => []
            ];
        }

        $pids = is_array($pids) ? $pids : [(int)$pids];

        // 通过设置场景来保证获取到的标题、URL、图片是符合要求的
        $sceneId = 0;
        $sceneAll = SceneService::all();
        foreach ($sceneAll as $scene) {
            if (in_array($siteId, $scene['sites']) && !empty($scene['status'])) {
                $sceneId = $scene['id'];
                break;
            }
        }

        unset($domains, $sceneAll);

        // 设置场景ID，不管是命令行调用还是前端调用均可
        SceneService::setConsoleSceneId($sceneId);
        SceneService::setFrontendSceneId($sceneId);

        $products = self::getProductsByIdsFromCache($pids);
        $data = [];
        foreach ($products as $product) {
            $url = data_get($product, 'url', '');
            if (!empty($url)) {
                if (Str::startsWith($domain, 'release.')) {
                    $url = 'https://m-' . $domain . $url;
                } elseif (Str::startsWith($domain, 'm-release.')) {
                    $url = 'https://' . $domain . $url;
                } else {
                    $url = 'https://m.' . $domain . $url;
                }
            }

            $featureImage = data_get($product, 'feature_image', []);
            $featureImage = array_column($featureImage, 'path');

            $data[] = [
                'name' => data_get($product, 'name', ''),
                'url' => $url,
                'img' => $featureImage,
            ];
        }

        if(empty($data)){
            return [
                'code' => 103,
                'msg' => '产品不在库或者不存在此产品！',
                'data' => []
            ];
        }

        return [
            'code' => 0,
            'msg' => 'SUCCESS',
            'data' => $data
        ];
    }


    /**
     * 设定每天凌晨2点统计二级分类近15天的销量排序
     * @return
     */
    public static function summaryCategoryProdutsIds(){
        //二级分类
        $categories_ids = DB::table("cc_categories")->where(['parent_id'=>1,'is_enabled'=>1])->lists('id');
        $categories_ids = DB::table("cc_categories")->whereIn('parent_id',$categories_ids)->where(['is_enabled'=>1])->lists('id');

        foreach($categories_ids as $cid){
            $cacheKey = '15day-cids-pids-' . $cid;
             Cache::tags('category-products')->rememberForever($cacheKey, function () use ($cid) {
                $hotSaleProduct = App::make('Jason\Ccshop\Controllers\Products')->getCategoryProducts($cid,
                    ['saled' => 'desc', 'recentlySaledDay' => 15], false, 0, 0, []);
                 return $hotSaleProduct['pids'];
             });
        }
        return true;
    }

    public static function getDingPlacedData($data){
        $placed_data = [];
        $errPids = [];
        foreach($data as $key=>$val){

            $date = Carbon::parse($val->created_at)->toDateString();
            $options = json_decode($val->options2,true);
            $pid = $val->product_id;
            $color = '';
            $size  = '';
            if(empty($options['カラー'][0])){
                $errPids[] = $pid;
            }

            $color = $options['カラー'][0] ?? '无颜色';

            if(!isset($placed_data[$pid][$color][$date])){
                $placed_data[$pid][$color][$date] = 0;
            }
            $placed_data[$pid][$color][$date] += $val->qty;

        }
        info('下单商品异常:'.json_encode($errPids));
        //近三天日期
        $date0 = Carbon::yesterDay()->subDays(2)->toDateString();
        $date1 = Carbon::yesterDay()->subDays(1)->toDateString();
        $date2 = Carbon::yesterDay()->toDateString();

        $result = [];
        $placed_limit = 3;
        $placed_total_limit = 20;
        foreach($placed_data as $pid=>$val){
            foreach($val as $color=>$value){
                if(array_sum($value) >= $placed_total_limit || (($value[$date0] ?? 0) >=  $placed_limit && ($value[$date1] ?? 0) >=  $placed_limit && ($value[$date2] ?? 0) >=  $placed_limit)){
                    $result[$pid][] = $color;
                }
            }
            
        }
        return $result;
    }


    public static function getDingPrintData($data,$ding_placed_data){
        $printing_data = [];//两天销量满足条件
        //近三天日期
        $date0 = Carbon::yesterDay()->subDays(2)->toDateString();
        $date1 = Carbon::yesterDay()->subDays(1)->toDateString();
        $date2 = Carbon::yesterDay()->toDateString();

        $print_data = $print_total_data = $print_total_options = [];
        $errPids = [];
        foreach($data as $key=>$val){
            $date = Carbon::parse($val->created_at)->toDateString();
            if($date == $date0){
                continue;
            }
            $options = json_decode($val->options2,true);
            $pid = $val->product_id;
            $color = '';
            $size  = '';
            if(empty($options['カラー'][0]) || empty($options['サイズ'][0])){
                $errPids[] = $pid;
            }
            $color = $options['カラー'][0] ?? '无颜色';
            $size  = $options['サイズ'][0] ?? '无尺码';

            $option_key = $color.'_'.$size;
            if(!isset($print_data[$pid][$date]['total'])){
                $print_data[$pid][$date]['total'] = 0;
            }

            if(!isset($print_data[$pid][$date]['options'][$option_key])){
                $print_data[$pid][$date]['options'][$option_key] = 0;
            }

            if(!isset($print_total_data[$pid][$option_key])){
                $print_total_data[$pid][$option_key] = 0;
            }


            if(!isset($print_total_options[$pid][$color][$size])){
                $print_total_options[$pid][$color][$size] = 0;
            }
            $print_total_options[$pid][$color][$size] += $val->qty;



            $print_data[$pid][$date]['options'][$option_key] += $val->qty;
            $print_data[$pid][$date]['total'] += $val->qty;

            $print_total_data[$pid][$option_key] += $val->qty;
        }
        info('打版异常商品:'.json_encode($errPids));
        //打版数据处理
        $print_limit = 5;
        $print_message = '';
        $print_pids = [];
        foreach($print_data as $pid=>$val){
            if(($print_data[$pid][$date2]['total'] ?? 0) < $print_limit || ($print_data[$pid][$date1]['total'] ?? 0) < $print_limit){
                //删除不满足条件商品
                unset($print_data[$pid]);
                continue;
            }
            $max_qty = 0;
            $max_sku = [];
            $max_color = [];
            $other_sku = [];
            foreach($print_total_data[$pid] as $key=>$qty){
                if($max_qty < $qty){
                    $max_sku = [$key];
                    $max_qty = $qty;
                }elseif($max_qty == $qty){
                    $max_sku[] = $key;
                }
                $other_sku[] = $key;
                
            }

            $max_color = [];
            foreach($max_sku as $sku){
                $max_color[] = explode('_',$sku)[0];
            }
            $other_color= [];
            foreach($other_sku as $sku){
                $other_color[] = explode('_',$sku)[0];
            }
            $max_color = array_unique(array_values($max_color));
            $other_color = array_unique(array_values($other_color));
            $diff_color = array_values(array_diff($other_color,$max_color));

            $effect_color = [];
            if(!empty($ding_placed_data[$pid])){
                $effect_color = array_intersect($diff_color,$ding_placed_data[$pid]);
            }

            $effect_sku = [];

            foreach($effect_color as $color){
                $maxx_qty = max($print_total_options[$pid][$color]);
                foreach($print_total_options[$pid][$color] as $size=>$qty){
                    if($maxx_qty == $qty){
                        $effect_sku[] = $color.'_'.$size;
                    }
                }
                

            }


            $print_pids[$pid]=['pid'=>$pid,'max_sku'=>$max_sku,'other_sku'=>$effect_sku];
        }
        return $print_pids;
    }

    public static function cronSendDingMsgNew(){
        $enabled_send_sales = env('APP_ENABLED_SEND_SALES', false);
        if(!$enabled_send_sales){
            return;
        }
        //特价商品(勾选位id=39的商品)销量满足条件发送钉钉消息
        $section_id = 369;
        $section_pids = DB::table('cc_product_section_selected')->where('section_id',$section_id)->lists('product_id');
        if(empty($section_pids)){
            return true;
        }
        $sub_days = 2;
        $begin_date = Carbon::yesterDay()->subDays($sub_days)->toDateTimeString();
        $end_date = Carbon::yesterDay()->endOfDay()->toDateTimeString();
        $statusIds = OrderStatus::saledStatusIds();

        $data = DB::table('cc_orders as o')->join('cc_order_products as p','o.id','=','p.order_id')
                ->whereIn('o.status_id',$statusIds)
                ->whereBetween('o.created_at',[$begin_date,$end_date])
                ->whereIn('p.product_id',$section_pids)
                ->select('p.product_id','p.options2','p.qty','o.created_at')
                ->get();
        $ding_placed_data = self::getDingPlacedData($data);
        $ding_print_data = self::getDingPrintData($data,$ding_placed_data);

        if(get('test_debug')){
            info('打版调试1-placed:'.json_encode($ding_placed_data));
            info('打版调试1-print:'.json_encode($ding_print_data));
        }
        //排除囤货商品
        $pids = array_merge(array_keys($ding_placed_data),array_keys($ding_print_data));
        $pids = array_values(array_unique($pids));

        if(empty(!$pids)){
            $products = DB::table('cc_products')->whereIn('id',$pids)->lists('spu','id');
            $spus = array_values($products);
            $new_erp_result = self::getStockSpusFromNewErp($spus);
            $old_erp_result = self::getStockSpusFromErp($spus);
    
            if(get('test_debug')){
                info('打版调试-new-erp:'.json_encode($new_erp_result));
                info('打版调试-old-erp:'.json_encode($old_erp_result));
            }
    
            if($new_erp_result['status'] != 200){
                info('打版和下单商品消息异常:'.PHP_EOL.'NEW-ERP获取囤货异常，具体请查看事件日志');
                return;
            }
    
            $stock_spu = array_merge($new_erp_result['stock_spu'] ?? [],$old_erp_result['stock_spu'] ?? []);
    
    
            //剔除囤货的商品
            foreach($ding_placed_data as $pid=>$val){
                $spu = $products[$pid] ?? '';
                if(in_array($spu,$stock_spu)){
                    unset($ding_placed_data[$pid]);
                    continue;
                }
            }
    
            foreach($ding_print_data as $pid=>$val){
                $spu = $products[$pid] ?? '';
                if(in_array($spu,$stock_spu)){
                    unset($ding_print_data[$pid]);
                    continue;
                }
            }
            if(get('test_debug')){
                info('打版调试2-placed:'.json_encode($ding_placed_data));
                info('打版调试2-print:'.json_encode($ding_print_data));
            }
        }else{
            $ding_placed_data = [];
            $ding_print_data = [];
        }
        self::sendPlacedDingMsg($ding_placed_data);
        self::sendPrintDingMsg($ding_print_data);
        return;
    }

    //发送囤货计划消息
    public static  function sendPurchasePlanMsg($sample_tank_message=''){
        $begin_date = $end_date = Carbon::parse()->yesterDay()->toDateString();
        //采购计划数据
        $purchase_plan_data = self::getPurchasePlanSpusFromNewErp($begin_date,$end_date);
        
        $message = '';
        //囤货流程数据
        $message .= '今日进入囤货流程:'.PHP_EOL;
        if(empty($purchase_plan_data['data'])){
            $message .= '无'.PHP_EOL;
        }else{
            foreach($purchase_plan_data['data'] as $key=>$val){
                if(empty($val['sku'])){
                    continue;
                }
                foreach($val['sku'] as $k=>$sku){
                    $message .= $sku.PHP_EOL;
                }
            }
        }
        $message .= ' '.PHP_EOL;
        //样衣池数据
        $message .= '今日进入样衣池:'.PHP_EOL;
        if(empty($sample_tank_message)){
            $message .= '无'.PHP_EOL;
        }else{
            $message .= $sample_tank_message.PHP_EOL;
        }
        $message .= ' '.PHP_EOL;
        /* 入库数据不处理，功能移到了产品库
        //入库数据
        $inbound_spus_data  = self::getInboundSpusFromNewErp($begin_date,$end_date);
        //定价spu数据，获取前六个月之内的spu入库数据
        $begin_date = Carbon::yesterDay()->subDays(10)->toDateString(); 
        $end_date = Carbon::yesterDay()->subDays(1)->toDateString();
        $inbound_spus = self::getInboundSpusFromNewErp($begin_date,$end_date);
        $pricing_spus = [];
        if($inbound_spus['status'] == 200){
            $pricing_spus = array_column($inbound_spus['data'],'spu');
        }
        $pricing_msg = '';
        $pricno_msg = '';
        foreach($inbound_spus_data['data'] as $key=>$val){
            if(in_array($val['spu'],$pricing_spus)){
                //已定价
                $msg = 'pricing_msg';
            }else{
                //未定价
                $msg = 'pricno_msg';
            }
            if(empty($val['sku'])){
                continue;
            }
            foreach($val['sku'] as $k=>$sku){
                $$msg .= $sku.PHP_EOL;
            }
        }
        //今日首次sku到货 spu已定价数据
        $message .= '今日首次sku到货，spu已经定过价的:'.PHP_EOL;
        if(empty($pricing_msg)){
            $message .= '无'.PHP_EOL;
        }else{
            $message .= $pricing_msg.PHP_EOL;
        }
        $message .= ' '.PHP_EOL;
        //今日首次sku到货 spu未定价数据
        $message .= '今日首次sku到货，spu没有定过价的:'.PHP_EOL;
        if(empty($pricno_msg)){
            $message .= '无'.PHP_EOL;
        }else{
            $message .= $pricno_msg.PHP_EOL;
        }
        */
        self::sendDingMessage($message,'purchase_plan');
        return;
    }

    //发送打版商品消息
    public static function sendPrintDingMsg($data){
        if(get('test_debug')){
            info('打版调试-print钉钉:'.json_encode($data));
        }

        $message = '';
        $plan_message = '';
        if(!empty($data)){
            $message .= ('销量连续2天大于等于5件，需要打版的SPU如下：'.PHP_EOL);
    
            $pids = array_keys($data);
    
            $products = DB::table('cc_products')->whereIn('id',$pids)->lists('spu','id');
    
            $records = DB::table('cc_send_print_records')->whereIn('product_id',$pids)->select('product_id','color')->get();
            $sended = [];
            foreach($records as $k=>$val){
                $sended[$val->product_id][] = $val->color;
            }
    
            $insert_data = [];
            $now_time = date('Y-m-d H:i:s');
            foreach($data as $pid=>$val){
                $spu = $products[$pid] ?? '';
    
                $send_sku = array_merge($val['max_sku'] ?? [],$val['other_sku'] ?? []);
    
                if(!empty($send_sku)){
                    foreach($send_sku as $sku){
                        $arr = explode('_',$sku);
                        if(isset($sended[$pid]) && in_array(($arr[0] ?? ""),$sended[$pid])){
                            continue;
                        }
                        $message .= ($spu.'   '.($arr[0] ?? "") .' '.($arr[1] ?? "")  .PHP_EOL);
                        $plan_message .= ($spu.'   '.($arr[0] ?? "") .' '.($arr[1] ?? "")  .PHP_EOL);
                        $insert_data[] = [
                            'product_id' => $pid,
                            'spu'        => $spu,
                            'color'      => $arr[0] ?? "",
                            'created_at' => $now_time,
                            'updated_at' => $now_time
                        ];
                    } 
                }
            }

            if(!empty($insert_data)){
                self::sendDingMessage($message);
                if(empty(get('test_debug'))){
                    DB::table('cc_send_print_records')->insert($insert_data);
                }
            }
            
        }
        //发送囤货计划数据
        self::sendPurchasePlanMsg($plan_message);
        return;


    }
    public static function getResponseFromErp($api='',$params=[]){
        $host = Settings::fetch('newerp_api_url', '');
        $arr = parse_url($host);
        $host = $arr['scheme'].'://'.$arr['host'];
        $api = $host.$api;
        $client = new Client([
            'base_uri' => $api,
            'timeout' => 600.0,
            'headers' => [
                'Accept' => 'application/json'
            ]
        ]);
        $isEnabledERPv3 = Settings::fetch('enable_erpv3_protocol', false);
        $secretId='2644af18ddc9781d0e3196a340f2d902';
        $secretKey = '46a4d6cb0af7e0be7edec39c338d658f';
        $signature = new Signature($secretId,$secretKey);
        $sign = $signature->setMethod('POST')->make($params);
        $parsedResponse = [];
        try{
            $response = $client->post($api. '?' . $sign, [
                RequestOptions::JSON => $params
            ]);
            // 请求的响应主体
            $responseContent = $response->getBody()->getContents();
            // 响应解析成数组
            $parsedResponse = json_decode($responseContent, true);
        } catch (\Exception $exception) {
            info('新erp接口请求异常：'.$exception->getMessage());
            return ['code'=>500,'data'=>[],'msg'=>$exception->getMessage()];
        }
        return $parsedResponse;

    }
    //获取新erp采购计划数据
    public static function getPurchasePlanSpusFromNewErp($begin_date='',$end_date=''){
        if(empty($begin_date) || empty($end_date)){
            $begin_date = $end_date = Carbon::parse()->yesterDay()->toDateString();
        }
        $params = ['begin_date'=>$begin_date,'end_date'=>$end_date];
        $status = 200;
        $data = [];
        //囤货接口地址
        $api = '/openapi/v1/stock/querySpuPlanDetail';
        $response = self::getResponseFromErp($api,$params);
        if(empty($response['code']) || $response['code'] != 200){
            info('获取新erp采购数据异常：'.json_encode($response));
            $status = 500;
        }else{
            $data = !empty($response['data']) ? $response['data'] : [];
        }
        return ['status'=>$status,'data'=>$data];

    }
    //获取新erp入库数据
    public static function getInboundSpusFromNewErp($begin_date='',$end_date=''){
        if(empty($begin_date) || empty($end_date)){
            $begin_date = $end_date = Carbon::parse()->yesterDay()->toDateString();
        }
        $params = ['begin_date'=>$begin_date,'end_date'=>$end_date];
        $status = 200;
        $data = [];
        //囤货接口地址
        $api = '/openapi/v1/stock/querySpuInboundStatus';
        $response = self::getResponseFromErp($api,$params);
        if(empty($response['code']) || $response['code'] != 200){
            info('获取新erp入库数据异常：'.json_encode($response));
            $status = 500;
        }else{
            $data = !empty($response['data']) ? $response['data'] : [];
        }
        return ['status'=>$status,'data'=>$data];
    }

    //获取新erp囤货数据
    public static function getStockSpusFromNewErp($spus){
        
        $begin_date = Carbon::now()->subDays(180)->toDateString(); 
        $end_date = Carbon::now()->toDateString();
        $params = ['spu'=>implode(',',$spus),'begin_date'=>$begin_date,'end_date'=>$end_date];
        $status = 200;
        $stock_spu = [];
        //囤货接口地址
        $api = '/openapi/v1/stock/querySpuStockStatus';
        $response = self::getResponseFromErp($api,$params);
        if(empty($response['code']) || $response['code'] != 200){
            info('获取erp囤货spu异常：'.json_encode($response));
            $status = 500;
        }else{
            $stock_spu = array_keys($response['data'] ?? []);
        }
        return ['status'=>$status,'stock_spu'=>$stock_spu];
    }

        //获取ERP囤货信息
    public static function getStockSpusFromErp($spus){
        $api = Settings::fetch('erp_api_url', '');
        $tokenData = DB::table('system_settings')
            ->where('item', 'erp_access_token')
            ->pluck('value');

        $token = json_decode($tokenData, true);
        $client = new Client([
                'base_uri' => $api,
                'timeout' => 600.0,
                'headers' => [
                    'Accept' => 'application/json',
                    'Authorization' => $token['token_type'] . ' ' . $token['access_token']
                ]
        ]);

        //请求旧erp接口，需要把spu首位9改回成0
        foreach($spus as $k=>$spu){
            $spus[$k] = substr_replace($spu,0,0,1);
        }

        $params = ['spu'=>implode(',',$spus)];
        $stock_spu = [];
        $status = 200;
        try {

            $response = $client->request('POST', 'stockpile/plan', [
                'form_params' => $params
            ]);
            // 请求的响应主体
            $responseContent = $response->getBody()->getContents();
            // 响应解析成数组
            $parsedResponse = json_decode($responseContent, true);


            if(empty($parsedResponse['code']) || $parsedResponse['code'] != 200){
                info('获取erp囤货spu异常：'.json_encode($parsedResponse));
                $status = 500;
            }else{
                $stock_spu = array_keys($parsedResponse['data'] ?? []);
            }
        } catch (\Exception $exception) {
            $status = 500;
            info('获取erp囤货spu异常：'.$exception->getMessage());
        }
        //旧erp返回的数据把 0 改成 9
        $return_spu = [];
        foreach($stock_spu as $k=>$spu){
            $return_spu[] = $spu;
            $return_spu[] = substr_replace($spu,9,0,1);
        }
        return ['status'=>$status,'stock_spu'=>$return_spu];
    }
    //发送下单商品消息
    public static function sendPlacedDingMsg($data){
        if(get('test_debug')){
            info('打版调试-placed钉钉:'.json_encode($data));
        }
        if(empty($data)){
            return;
        }
        $message = '';
        $message .= ('连续3天销量大于等于3件，或者3天内总销量大于20件，需要下单的SPU如下：:'.PHP_EOL);

        $pids = array_keys($data);

        $products = DB::table('cc_products')->whereIn('id',$pids)->lists('spu','id');
        
        $records = DB::table('cc_send_placed_records')->whereIn('product_id',$pids)->select('product_id','color')->get();
        $sended = [];
        foreach($records as $k=>$val){
            $sended[$val->product_id][] = $val->color;
        }
        
        $insert_data = [];
        $now_time = date('Y-m-d H:i:s');

        foreach($data as $pid=>$val){
            $spu = $products[$pid] ?? '';
            foreach($val as $k=>$color){
                if(isset($sended[$pid]) && in_array($color,$sended[$pid])){
                    continue;
                }

                $message .= ($spu.'   '.$color.PHP_EOL);

                $insert_data[] = [
                    'product_id' => $pid,
                    'spu'        => $spu,
                    'color'      => $color,
                    'created_at' => $now_time,
                    'updated_at' => $now_time
                ];
            } 
        }
        if(get('test_debug')){
            info('打版调试3-placed:'.json_encode($insert_data));
        }
        if(empty($insert_data)){
            return;
        }

        self::sendDingMessage($message);

        if(get('test_debug')){
            return;
        }

        DB::table('cc_send_placed_records')->insert($insert_data);

        return;

    }
    //发送钉钉消息
    public static function sendDingMessage($message,$type='print_placed'){
        if(get('test_debug')){
            info('打版钉钉消息调试:'.$message);
            return;
        }
        
        
        //$secret = 'SECef9002f5738cdafb71cdf1191ee7b436d7ef79edc84f329db8300d6229423859';
        //$accessToken = '8913f96044abf4b2102810fbc795e430e619b2a150fb9454800eb940e17917f7';
        if($type == 'print_placed'){
            $secret = Settings::fetch('dingtalk_print_placed_secret', '');
            $accessToken  = Settings::fetch('dingtalk_print_placed_webhook', '');
        }elseif($type == 'product_stockout'){//没有库存推送通知
            $secret = Settings::fetch('dingtalk_product_stockout_secret', '');
            $accessToken  = Settings::fetch('dingtalk_product_stockout_webhook', '');
        }else{
            $secret = Settings::fetch('dingtalk_purchase_plan_secret', '');
            $accessToken  = Settings::fetch('dingtalk_purchase_plan_webhook', '');
        }


        $dingTalkClient = DingTalkClient::client($accessToken, $secret);
        $dingTalkMessage = $dingTalkClient->makeTextMessage();
        $dingTalkMessage->setContent($message);
        $dingTalkClient->send();
        return true;
    }

    // 特价品 选项值换算|sku值替换
    public static function handleOptionsPrice(&$options,&$skus,$price){
        $sku_option_values = $skus;
        foreach ($sku_option_values as $k => $item){
            $option_values = $item['option_values'];
            $option_values_keys = array_keys($option_values);

            $lower = array_keys($option_values[$option_values_keys[0]])[0];

            $sku_option_values[$k]['lower'] = $lower;
        }
        $sku_option_values = array_column($sku_option_values,null,'lower');

        $optionPrice = [];
        foreach ($sku_option_values as $k => $v){
            $spe_price = $v['prices']['spe_price']??0;
            $option_price = $spe_price - $price;
            $optionPrice[$k]['price_variate'] = $option_price < 0 ? '-' : '+';
            $optionPrice[$k]['variate_value'] = str_replace("-",'',$option_price);
        }

        foreach ($options as &$option) {
            foreach ($option['values'] as &$optionValue) {
                $optionValue['bargain_price_variate'] = $optionPrice[$optionValue['id']]['price_variate']??'+';
                $optionValue['bargain_variate_value'] = $optionPrice[$optionValue['id']]['variate_value']??0;
            }
        }

        foreach ($skus as &$sku) {
            $sku['sku_price'] = $sku['prices']['spe_price']??0;
        }

        return true;
    }

    /**
     * 发送商品没有库存状态通知
     * @param $id
     * @param $spu
     * @param $backendUser
     * @return void
     */
    public static function sendStockoutDingMsg($id,$status,$backendUser='',$user=''){
        $statusArray = [
            'instock' => '在库',
            'stockout' => '没有库存',
            'unpublished' => '未发布',
            'disabled' => '已禁用',
            'untreated' => '未处理',
            'shelve' => '下架',
            'stocktension' => '库存紧张',
            'assessment' => '待评定',
            'avlremind' => '到货提醒',
            'default' => '未找到',
        ];

        $message  = '当前站点：'. config('app.url').PHP_EOL;
        $message .= '商品ID：'. $id.PHP_EOL;
        $message .= '商品SPU：'. DB::table('cc_products')->where('id', $id)->value('spu').PHP_EOL;
        $message .= '商品前状态：'. $statusArray[$status].PHP_EOL;
        $message .= '商品当前状态：没有库存'.PHP_EOL;
        $message .= '操作人员：'. ($user ? $user : ($backendUser?$backendUser->last_name.$backendUser->first_name:'')).PHP_EOL;
        $message .= '操作时间：'. Carbon::now('Asia/Shanghai')->toDateTimeString();

        self::sendDingMessage($message,'product_stockout');
    }

    /**
     * 获取最近销售的产品ID
     * @param $days
     * @param int $limit
     * @param array $cids
     * @param array $excludeIds
     * @param array $order
     * @param string $dateRange 指定日期区间 ['Y-m-d', 'Y-m-d']|['2021-10-01', '2021-1-30'] rsky-add
     * @param array $excludeCids 需要排除分类id
     * @param int $siteId 需要查询子域名id
     * @param array $priceRange 产品价格区间(筛选这个范围的价格产品) 格式：[1,1000]
     * @param int  $idSort 0： 默认id升序排序  1： id降序排序
     * @param int  $productName 产品标题
     * **********************************************
     * 线上的订单索引的mapping不支持模糊匹配产品标题
     * 若需要使用产品标题作为筛选条件
     * 要将线上现有的订单索引删除并重建，以改变mapping
     * *********************************************
     * @return mixed
     */
    public static function getRecentlySaledProductIdsFromDb(
        $days,
        $limit = 600,
        $cids = [],
        $excludeIds = [],
        $order = [],
        $dateRange = [],
        $excludeCids = [],
        $siteId = 0,
        $priceRange = [],
        $idSort = 0,
        $productName = ""
    )
    {
        $limit = (int)$limit;

        $keyStr = $days . $limit . json_encode($cids);

        // rsky-add 2021-2-20 存在指定结束日期，将其放入缓存key值，以作区分
        if ($dateRange) {
            $keyStr = $keyStr . implode('--', $dateRange);
        }

        if ($excludeIds) {
            $excludeIds = array_unique($excludeIds);
            sort($excludeIds);
            $keyStr = $keyStr . implode('-', $excludeIds);
        }

        if ($excludeCids) {
            sort($excludeCids);
            $keyStr = $keyStr . implode('-', $excludeCids);
        }

        if ($siteId) {
            $keyStr = $keyStr . $siteId;
        }

        if ($priceRange) {
            $keyStr = $keyStr . implode('-', $priceRange);
        }

        if ($idSort) {
            $keyStr = $keyStr . $idSort;
        }

        if ($productName) {
            $keyStr = $keyStr . $productName;
        }
        $cacheKey = md5($keyStr);
        if (isset($order['field']) && $order['field'] == 'order_id') {
            $tags = 'recently-placed-product-ids';
            $statusIds = OrderStatus::placedStatusIds();
        } else {
            $tags = 'recently-saled-product-ids';
            $statusIds = [];
        }
        $expiredTime = Helper::getRandExpiredTime(360);
        return Cache::tags($tags)->remember(
            $cacheKey . '-FromDb', $expiredTime,
            function () use (
                $days,
                $limit,
                $cids,
                $excludeIds,
                $statusIds,
                $order,
                $dateRange,
                $excludeCids,
                $siteId,
                $priceRange,
                $idSort,
                $productName
            ) {
                $productSales = OrderService::getRecentlySaledProductIdsFromDb($days, $limit + count($excludeIds),
                    $cids, $statusIds, $order, $dateRange, $excludeCids, $siteId, $productName);
                $pids = array_keys($productSales);
                $pids = array_diff($pids, $excludeIds);
                if ($excludeIds) {
                    array_splice($pids, $limit);
                }
                $query = DB::table('cc_products')
                    ->whereIn('cc_products.id', $pids)
                    ->where('cc_products.status', 'instock');
                if (!empty($priceRange) && count($priceRange) > 1) {
                    $query->whereBetween('cc_products.price', $priceRange);
                }
                if (count($cids) > 0) {
                    $query->join('cc_product_categories', 'cc_products.id', '=', 'cc_product_categories.product_id');
                    $query->whereIn('cc_product_categories.category_id', $cids);
                }
                $instockPids = $query->lists('cc_products.id');

                $instockSales = [];
                foreach ($instockPids as $pid) {
                    if (array_key_exists($pid, $productSales)) {
                        $instockSales[$pid] = $productSales[$pid];
                    }
                }
                $keys = array_keys($instockSales);

                $vals = array_values($instockSales);

                if ($idSort) {
                    array_multisort($vals, SORT_DESC, $keys, SORT_DESC);
                } else {
                    array_multisort($vals, SORT_DESC, $keys, SORT_ASC);
                }

                $instockSales = array_combine($keys, $vals);

                return $instockSales;
            }
        );
    }

    /**
     * 将原来获取分类产品id的方法单独封装
     * @param mixed $category 分类
     * @param  array  $order 排序
     * @param  int  $limit 限制数量
     * @param  int  $subsite 域名id
     * @param  false  $isNew 是否新品排序
     * @param  false  $isStar 是否标星
     * @param  int  $isSelfAndChildrenIds 是否包含子分类
     * @param  array  $region 价格范围
     * @param  array  $features 特增
     * @param  array  $options 选项
     *
     * @return mixed|array
     */

    public static function getCategoryProductIdsFromDb($category, $order = [], $limit = 0, $subsite = 0, $isNew = false, $isStar = false, $isSelfAndChildrenIds = 0, $region = [], $features = [], array $options = [])
    {
        $cid = null;
        if (is_numeric($category)) {
            $cid = (int)$category;
        } elseif (!empty($category['id'])) {
            $cid = (int)$category['id'];
        }
        if (!$cid){
            return [];
        }
        $expiredTime = Helper::getRandExpiredTime(360);
        $cacheKey = Categories::getDefaultCachekey($cid, $subsite, $isNew, $order, $limit) . md5(json_encode([
                $isStar,
                $isSelfAndChildrenIds,
                $region,
                $features,
                $options
            ]));
        return \Cache::tags(['category-products', 'category-products-' . $cid])->remember($cacheKey.'-FromDb', $expiredTime, function () use ($category, $order, $isStar, $isSelfAndChildrenIds, $limit ,$region, $features, $isNew, $subsite, $options) {
            return (new Products())->flushCategoryProductsFromDb($category, $order, $isStar, $isSelfAndChildrenIds, $limit ,$region, $features, $isNew, $subsite, $options);
        });
    }

    /**
     * 从缓存中获取产品的价格信息
     * @param $pid
     * @param $scene_id
     *
     * @return array|mixed
     */
    public static function getDbProductFromCache($pid, $scene_id)
    {
        $enabledProductSkuPrice = Settings::fetch('is_enable_product_sku_price');
        if ($enabledProductSkuPrice){
            $cacheKey = $pid.'-'.$scene_id . $enabledProductSkuPrice;
        }else{
            $cacheKey = $pid.'-'.$scene_id;
        }

        static $enabledPromotionIdSort;
        $enabledPromotionIdSort = $enabledPromotionIdSort ?? PromotionService::enabledPromotionIdSort();
        if ($enabledPromotionIdSort) {
            $specialSortIds = PromotionService::getPromotionIdSortIds();
            if (in_array($pid, $specialSortIds)) {
                $cacheKey .= '-special-price';
            }
        }

        PromotionService::pushPromoIdSortPriceCacheKey($pid, $cacheKey);

        if(!empty(self::$dbProductInfos[$cacheKey])){
            return self::$dbProductInfos[$cacheKey];
        }
        $expiredTime = Helper::getRandExpiredTime(180,180);
        $productInfo = \Cache::tags('products.dbProductInfo')->remember($cacheKey, $expiredTime, function () use ($pid, $scene_id) {
            $product = Product::where('id', $pid)->select('id', 'price', 'list_price','instock_time', 'special_price')->first();
            if(empty($product)){
                return [];
            }
            return ProductService::getDbProductInfo($product, $scene_id);
        });
        self::$dbProductInfos[$cacheKey] = $productInfo;
        return $productInfo;
    }

    /**
     * 将产品中的sku_price与传入的sku做匹配，返回对应的sku价格
     * @param $product
     * @param  string  $sku
     *
     * @return array
     */
    public static function formatProductSkuPrice($product, $sku = '')
    {
        $enabledProductSkuPrice = Settings::fetch('is_enable_product_sku_price');
        if($sku && $enabledProductSkuPrice){
            $productInfo = [
                'price' => $product['sku_price'][$sku] ?? $product['price'],
                'list_price' => $product['sku_list_price'][$sku] ?? $product['list_price'],
                'src_price' => $product['sku_src_price'][$sku] ?? $product['src_price'],
                'promotion_price' => $product['sku_promotion_price'][$sku] ?? $product['promotion_price'],
            ];
            $productInfo['discount'] = (new Product)->getDiscount($productInfo['list_price'], $productInfo['price'], $productInfo['promotion_price']);
        }else{
            $productInfo = [
                'price' => $product['price'] ?? 0,
                'list_price' => $product['list_price'] ?? 0,
                'src_price'  => $product['src_price'] ?? 0,
                'promotion_price'  => $product['promotion_price'] ?? 0,
            ];
            $productInfo['discount'] = $product['discount'] ?? (new Product)->getDiscount($productInfo['list_price'], $productInfo['price'], $productInfo['promotion_price']);
        }
        return $productInfo;
    }

    /**
     * 根据传入的产品数据以及场景id，获取指定场景的sku价格数组
     * @param $product
     * @param $scene_id
     *
     * @return array
     */
    public static function getDbProductInfo($product, $scene_id)
    {
        if ($scene_id){
            $sceneData = $product->getSceneData($scene_id);
            $data = [
                'price' => $sceneData['price'] ?? $product->price,
                'list_price' => $sceneData['list_price'] ?? $product->list_price,
                'src_price' => $sceneData['src_price'] ?? $product->src_price,
                'promotion_price' => $sceneData['promotion_price'] ?? $product->promotion_price,
                'discount' => $sceneData['discount'] ?? $product->discount
            ];
            if(Settings::fetch('is_enable_product_sku_price')){
                return array_merge($data, [
                    'sku_price' => $sceneData['sku_price'] ?? [],
                    'sku_src_price' => $sceneData['sku_src_price'] ?? [],
                    'sku_list_price' => $sceneData['sku_list_price'] ?? [],
                    'sku_promotion_price' => $sceneData['sku_price'] ?? [],
                ]);
            }
            return $data;
        }else{
            return $product->toArray();
        }
    }

    /**
     * @param array $pids
     * @return array
     * 提供matomo id置顶原始链接（只保留id置顶参数），以及返回链接token
     */
    public static function getIdSortUrlHash($pids = [])
    {
        $url = \Request::url();
        $getIdSort = get('id_sort', '');
        return ['url' => $url . '?id_sort=' . $getIdSort, 'token' => md5($url . json_encode($pids))];
    }

    public static function getIdsSortPida($idSortRecommend = false): array
    {
        $pids     = trim(get('id_sort', ''),',');
        $uniqueId = trim(get('uniqueId', ''), ',');

        $specialIds = PromotionService::getSpecialSortIds();
        PromotionService::savePromotionIdSortIds($specialIds);

        $pids = implode(',', $specialIds) . ',' . $pids;
        $pids = trim($pids, ',');

        if (empty($pids) && empty($uniqueId)) {
            return [];
        }

        if (!is_string($pids)) {
            return [];
        }

        $pids = urldecode($pids);
        $pidas = $pids ? explode(',', $pids) : [];

        if (Helper::isMyiume()) {
            $siteId = MoresiteService::getCurrentSiteId();
            $siteInfo =current(MoresiteService::getSitesBySiteIds([$siteId]));

            $conceal = 0;
            if(!empty($siteInfo['conceal']) && empty($siteInfo['is_main'])){
                $conceal =  $siteInfo['conceal'];
            }
            foreach($pidas as $k=>$pid){
                $pidas[$k] = Intval($pid) - $conceal;
            }
        }

        // 组合unique置顶数据
        $uniquePids = RedirectSetting::getIdSorts($uniqueId);
        $pidas      = array_merge($pidas, $uniquePids);
        $pida       = [];
        foreach ($pidas as $pid) {
            $pida[] = (int)trim($pid);
        }
        $pida        = array_unique(array_filter($pida));
        $beforeCount = count($pida);
        $pida        = array_filter($pida, static function ($item) {
            return is_numeric($item) && $item < 2147483647;
        });
        $afterCount  = count($pida);
        if ($beforeCount > $afterCount) {
            $_url = request()->fullUrl();
            info('id置顶投放信息有误，来源url：' . $_url . PHP_EOL);
        }

//        $enabledAdProductWhitelist = (bool)Settings::fetch('enabled_ad_product_whitelist');
//        if ($afterCount > 1 && $enabledAdProductWhitelist && $channel = AdProductWhitelist::getAdSourceChannel()) {
//            $adProductWhitelist = AdProductWhitelist::fetch($channel, 0) ?: [];
//            if (is_array($adProductWhitelist) && !empty($adProductWhitelist)) {
//                $pida = array_values(array_diff($pida, $adProductWhitelist));
//            }
//        }

        //后台手动配置推荐商品模块
        $recommendMatomoIds = $pida;
        $idSortRelateIds = $idSortRecommendContact = [];
        if (Settings::fetch('enabled_manual_recommend_products') && (int)Settings::fetch('recommend_manual_list_number', 0) > 0) {
            $manualIdSortIds = self::manualIdSortIds($pida);  //获取idsort手动推荐
            $idSortRelateIds = $manualIdSortIds['idSortRelateIds'];  //手动推荐后的结果ID集合
            $idSortRecommendContact = $manualIdSortIds['idSortRecommendContact']; //原始商品与手动推荐ID绑定关系
            $recommendMatomoIds = $manualIdSortIds['recommendMatomoIds']; //留给matomo获取推荐的ID
        }

        //大数据推荐模块
        $afterCount = count($recommendMatomoIds);
        $maxNumber = 0;
        $hasMany = false;
        $recommend = $idSortMatomoRecommendContact = [];
        if (
            (
                // 只有一个置顶
                $afterCount === 1
                // 或者有多个置顶，并且配置了最大数量
                || $hasMany = ($afterCount > 1 && ($maxNumber = (int)Settings::fetch(
                        'recommend_matomo_list_max_number'
                    )) > 0)
            )
            && Settings::fetch('enabled_matomo_recommend_products')
            // 如果 re_num 是纯数字，那就转为数字，否则就取默认值
            && ($reNum = ctype_digit($reNum = get('re_num')) ? (int)$reNum : (int)Settings::fetch('recommend_matomo_list_number')) > 0
        ) {
            $options = [];
            (null === ($mode = get('opt'))) || $options['mode'] = (int)$mode;
            (null === ($threshold = get('sim'))) || $options['threshold'] = (float)$threshold;
            (null === ($dbSource = get('db_source'))) || $options['db_source'] = (string)$dbSource;

            $excludes = array_values(array_unique(array_merge($pida,$idSortRelateIds))); //原始ID以及手动推荐过后的商品不出现在推荐结果中
            $recommendData = self::getIdSortMatomoRecommendIdsNew($recommendMatomoIds,$reNum,$options,$excludes);
            $recommend = $recommendData['ids'];
            $idSortMatomoRecommendContact = $recommendData['contact'];

            if ($hasMany) {
                $recommend = array_slice($recommend, 0, max($maxNumber, $reNum));
            }
        }

        //手动推荐模块与大数据推荐模块推荐之后混合排序，以及分类手动推荐与大数据推荐ID
       // $manualMotomoIdSort = self::manualMotomoIdSortIds($pida,$idSortRelateIds, $recommend,$idSortRecommendContact,$idSortMatomoRecommendContact);

       // if ($idSortRecommend) {
        if (false) {
            $mode = (int)Settings::fetch('matomo_recommend_result_mode',1);
            //只有算法推荐数据才发送给matomo
            if ($mode != 1) {
                return ['ids' => $manualMotomoIdSort['ids']];  //只提供ID返回
            }
            //提供前台token信息，以及推荐商品的ID信息
            $urlToken = self::getIdSortUrlHash($manualMotomoIdSort['ids']);
            $idSortRecommendData = $idSortRecommendNewData =[
                'ids' => $manualMotomoIdSort['ids'],
                'id_sort' => $pids,
                'recommendIds' => $manualMotomoIdSort['recommendIds'],
                'url' => $urlToken['url'],
                'token' => $urlToken['token'],
                'rec_ccshop' => [
                    'details' => $manualMotomoIdSort['idSortRecommendContact'],
                    'total' => $manualMotomoIdSort['idSortRecommendIds'],
                ],
                'rec_matomo' => [
                    'details' => $manualMotomoIdSort['idSortMatomoRecommendContact'],
                    'total' => $manualMotomoIdSort['idSortMatomoRecommendIds'],
                ]
            ];
            // 提供matomo同步此表数据计算推荐商品关联关系
            if(!empty($manualMotomoIdSort['recommendIds'])){
                unset($idSortRecommendNewData['recommendIds']);
                unset($idSortRecommendNewData['ids']);
                if(empty($idSortRecommendNewData['rec_ccshop']['details'])){
                    $idSortRecommendNewData['rec_ccshop']['details'] = new \stdClass();
                }
                if(empty($idSortRecommendNewData['rec_matomo']['details'])){
                    $idSortRecommendNewData['rec_matomo']['details'] = new \stdClass();
                }
              //  IdSortRecLog::updateOrCreate(['token' => $urlToken['token']], ['details' => json_encode($idSortRecommendNewData) ,'created_at'=> date('Y-m-d H:i:s',time())]);
            }
            return  $idSortRecommendData;
        }

        return $manualMotomoIdSort['ids'];
    }

    /**
     * @param array $recommendMatomoIds
     * @param int $reNum
     * @param array $options
     * @return array|array[]
     * id置顶不同模式推荐算法
     */
    public static function getIdSortMatomoRecommendIdsNew($recommendMatomoIds = [], $reNum = 0, $options = [],$excludes = [])
    {

        if (empty($recommendMatomoIds)) {
            return ['ids' => [], 'contact' => []];
        }

        $mode = (int)($options['mode'] ?? Settings::fetch('matomo_recommend_result_mode') ?: 1);
        $contact = $recommend = [];
        //matomo推荐重构，采集系统还用原先的逻辑
        if ($mode == 1) {
            //matomo推荐重构
            $options['excludes'] = $excludes;
            $matomoRecommend = self::queryMatomoRecommendProductsIdsNew($recommendMatomoIds,$reNum,$options);
            $recommend = $matomoRecommend['ids'];
            $contact = $matomoRecommend['contact'];
        } elseif($mode == 3) {
            //长春二部采集系统推荐
            foreach ($recommendMatomoIds as $id) {
                $options['excludes'] = array_merge($excludes, $recommend);
                /** @noinspection SlowArrayOperationsInLoopInspection */
                $matomoSelectIds = self::queryMatomoRecommendProductsId($id, $reNum, false, $options);
                $contact[$id] = $matomoSelectIds; //原始商品与matomo推荐ID绑定关系
                $recommend = array_merge($recommend, $matomoSelectIds);
            }
        }

        return ['ids' => $recommend, 'contact' => $contact];
    }

    /**
     * @param array $pids
     * @param int $reNum
     * @param array $options
     * @return array|array[]
     * 获取matomo推荐商品方法
     */
    public static function queryMatomoRecommendProductsIdsNew($pids = [], $reNum = 0,$options = [])
    {

        //推荐结果不需要$excludes这些商品
        $excludes = $ignored = Helper::split((string)Settings::fetch('enabled_matomo_recommend_products_excludes'));
        $excludes = array_merge($excludes, $options['excludes'] ?? []);
        $excludes = array_map('intval', $excludes);

        //仅这些产品会显示推荐产品
        $allows = (array)($options['allows'] ?? Helper::split((string)Settings::fetch('recommend_matomo_allows')));
        $allows = array_map('intval', $allows);
        // 如果 allows 不为空，那就和要查询的产品取交集
        if (!empty($allows)) {
            $pids = array_intersect($pids, $allows);
        }

        //仅推荐最近 15 天下单数>=N以上的商品
        $ordersLimit = (int)($options['orders_limit'] ?? Settings::fetch('recommend_matomo_min_orders_limit'));
        if ($ordersLimit > 0) {
            // 查询订单量（依赖 Redis）
            $productRealSales = self::getProductRealSales($pids, 15, 'placed');
            $pids = array_keys(
                array_filter($productRealSales, static function ($sales) use ($ordersLimit) {
                    return $sales >= $ordersLimit;
                })
            );
            $pids = array_map('intval', $pids);
        }

        //仅推荐在库时间大于N天的商品
        $instockLimit = (int)($options['instock_limit'] ?? Settings::fetch('new_matomo_min_orders_limit'));
        if ($instockLimit > 0) {
            $productsInstock = ProductService::getProductsByIdsFromCache($pids);
            $productsInstock = array_column($productsInstock, 'instock_time', 'id');
            $instockLimit = Carbon::now()->subDays($instockLimit)->toDateTimeString();

            $pids = array_keys(array_filter($productsInstock, static function ($data) use ($instockLimit) {
                return $data < $instockLimit;
            }));
            $pids = array_map('intval', $pids);
        }


        // 如果空
        if (empty($pids)) {
            return ['ids' => [], 'contact' => []];
        }


        //region 从 Matomo 接口获取产品推荐
        $domain = $options['domain'] ?? TraceCode::fetch('matomo_tracker_url');
        $siteId = (int)($options['site_id'] ?? TraceCode::fetch('matomo_site_id'));
        $reNum = (int)Settings::fetch('recommend_matomo_list_max_number',0);

        $payload = [
            'matomo_domain' => $domain,
            'matomo_site_id' => $siteId,
            'pid' => implode(',', $pids),
            'qty' => $reNum,
        ];

        if (count($excludes)) {
            $payload['exclude_pids'] = implode(',', $excludes);
        }

        $address = (string)Settings::fetch('matomo_recommend_remote_address') ?: '172.31.54.111';
        $port = (int)Settings::fetch('matomo_recommend_remote_port') ?: '29090';
        $sendTimeout = (int)Settings::fetch('matomo_recommend_send_timeout') ?: 100000;
        $receiveTimeout = (int)Settings::fetch('matomo_recommend_receive_timeout') ?: 10000;

        $server = new MartClient(
            $address, $port,
            ['send_timeout' => $sendTimeout, 'receive_timeout' => $receiveTimeout]
        );

        $response = Helper::requestMartDB(
            $payload,
            'RecommendRelatedProductsHandler',
            'getRecommendRelatedProductsResult',
            $server
        );

        $response = json_decode($response, true) ?: [];

        $ids = explode(',',$response['rec_result']['cf'] ?? '');
        $ids = $oIds = array_map('intval', $ids);
        $contact = [];
        foreach ($response['rec_result']['cf_details'] ?? [] as $key => $val) {
            $contact[$key] = !empty($val) ? array_map('intval', explode(',', $val)) : [];
        }

        $ids = array_diff($ids,$excludes);
        $ids = array_values(array_unique($ids));
        $ids = array_slice($ids, 0, $reNum);

        if(get('test',0) == 1){
            info(implode(',', $pids).'matomo推荐ID:'.implode(',', $ids).'matomo原始推荐ID:'.implode(',', $oIds));
            info('matomo推荐关系:'.json_encode($contact));
        }
        return ['ids' => $ids, 'contact' => $contact];

    }

    /**
     * 从 matomo 拿推荐产品ID
     *
     * @param int|int[]|string $pid   产品ID
     * @param int       $limit 限制数量，默认不限制
     * @param bool      $aggs  传0的时候  就是返回每个产品各自的结果  不传或者传其他值就是返回整体聚合的一个结果
     * @param array{cacheable:bool, mode:int, threshold:float, domain:string, site_id:int} $options
     *
     * @return array|int[]
     */
    public static function queryMatomoRecommendProductsId($pid, int $limit = 0, bool $aggs = false, array $options = []): array
    {
        if (is_string($pid) && strpos($pid, ',') !== false) {
            $pid = Helper::split($pid);
        }

        if (!($isArray = is_array($pid))) {
            $pid = [$pid];
        }

        $pid = array_map('intval', $pid);

        // 推荐的模式
        $mode = (int)($options['mode'] ?? Settings::fetch('matomo_recommend_result_mode') ?: 1);
        in_array($mode, [1, 2]) && $aggs = true;

        // 如何返回空实体
        $emptyEntity = ($isArray && !$aggs) ? array_fill_keys($pid, []) : [];

        if (empty($pid) || !Settings::fetch('enabled_matomo_recommend_products')) {
            return $emptyEntity;
        }

        // 要排除的产品
        $excludes = $ignored = Helper::split((string)Settings::fetch('enabled_matomo_recommend_products_excludes'));
        $ignored = array_merge($ignored, $options['ignored'] ?? []);
        $ignored = array_map('intval', $ignored);
        $pid = array_values(array_diff($pid, $ignored));

        $excludes = array_merge($excludes, $options['excludes'] ?? []);
        $excludes = array_map('intval', $excludes);
        $excludesCount = count($excludes);

        // limit > 0 并且要排除的数量大于 0
        $canExclude = $excludesCount > 0;
        // 如果 可以排除 ，那就把 limit+要排除的产品数量
        $tempLimit = ($canExclude && $limit > 0) ? $limit + $excludesCount : $limit;
        $productIds = self::buildMatomoRecommendProductsId($pid, $tempLimit, $options);

        // 如果要排除的数量大于 0
        if ($canExclude) {
            $productIds = array_map(static function ($productIds) use ($excludes) {
                return array_diff($productIds, $excludes);
            }, $productIds);
        }

        // 限制获取的 limit，查询接口的内部不再限制
        $productIds = array_map(static function ($productId) use ($limit) {
            return $limit > 0 ? array_slice($productId, 0, $limit) : $productId;
        }, $productIds);

        if (empty($productIds)) {
            return $emptyEntity;
        }

        if (!$isArray || $aggs) {
            return current($productIds) ?: [];
        }

        return array_replace($emptyEntity, $productIds);
    }

    /**
     * @param $pida
     * @return array
     * id置顶获取手动关联商品
     */
    public static function manualIdSortIds($pida)
    {

        $recommendMatomoIds = $idSortRecommendContact = [];
        $manualOnlyIdsString = Settings::fetch('recommend_manual_allows', '');
        $manualOnlyIds = explode(',', $manualOnlyIdsString); //只针对这批ID进行手动推荐
        $manualSort = Settings::fetch('recommend_manual_sort', 1); //默认为1在库时间排，2为15天销量排序
        $recommendManualListNumber = (int)Settings::fetch('recommend_manual_list_number'); //每个产品取X个推荐
        $recommendManualListMaxNumber = (int)Settings::fetch('recommend_manual_list_max_number'); //每个产品X个推荐后重新排序后，取X个

        $sortPida = $pida;
        if ($manualOnlyIdsString) {
            $sortPida = array_intersect($sortPida, $manualOnlyIds); // 符合手动推荐的置顶id
        }

        $recommendMatomoIds = array_diff($pida, $sortPida); //idsort不满足手动推荐的ID，使用matomo接口获取推荐

        sort($sortPida);
        $cacheKey = md5(json_encode($sortPida) . '-' . json_encode($pida) .'-'. $manualSort . '-' . $recommendManualListNumber . '-' . $recommendManualListMaxNumber);

        $idSortRelateData = Cache::tags('products-recommend')->remember($cacheKey, 1440, function () use ($sortPida, $manualSort, $recommendManualListNumber, $recommendManualListMaxNumber,$pida) {
            $idSortRelateIds = $idSortRecommendContact = [];
            $query = ProductRelate::query()
                ->whereIn('cc_product_related_to_products.related_id', $sortPida) //需要获取关联的产品
                ->join('cc_products', 'cc_products.id', '=', 'cc_product_related_to_products.product_id')
                ->where('cc_products.status', 'instock');

            if ($manualSort == 2) {
                $query->leftJoin('cc_product_order_stats', function ($q) {
                    $q->on('cc_product_order_stats.product_id', '=', 'cc_product_related_to_products.product_id')
                        ->where('cc_product_order_stats.day', '=', 15)
                        ->where('cc_product_order_stats.type', '=', 'saled');
                })->orderBy('cc_product_order_stats.amount', 'desc')
                    ->select('cc_product_related_to_products.product_id', 'cc_product_related_to_products.related_id','cc_products.instock_time','cc_product_order_stats.amount');
            } else {
                $query->select('cc_product_related_to_products.product_id', 'cc_product_related_to_products.related_id','cc_products.instock_time');
            }
            $idSortRelate = $query->orderBy('cc_products.instock_time', 'desc')->get();
            $idSortRelate = $idSortRelate->groupBy('related_id')->toArray();  //分组，每一个置顶ID为一组
            if ($idSortRelate) {
                $idSortRelateLimit = [];
                //获取每个商品中排序好的关联商品，取出前X个，再次重新排序
                foreach ($idSortRelate as $key=> $value) {
                    $value = array_slice($value, 0, $recommendManualListNumber);
                    $idSortRecommendContact[$key] = array_column($value,'product_id');  //原始ID => 推荐ID
                    $idSortRelateLimit = array_merge($idSortRelateLimit, $value);
                }

                if ($manualSort == 1) {  //新品排序
                    $instockArr = array_column($idSortRelateLimit, 'instock_time');
                    array_multisort($instockArr, SORT_STRING, SORT_DESC, $idSortRelateLimit);
                } elseif ($manualSort == 2) {  //15天销量排序
                    $placedArr = array_column($idSortRelateLimit, 'amount');
                    $instockArr = array_column($idSortRelateLimit, 'instock_time');
                    array_multisort($placedArr, SORT_NUMERIC, SORT_DESC, $instockArr, SORT_STRING, SORT_DESC, $idSortRelateLimit);
                }

                $idSortRelateLimit = array_unique(array_column($idSortRelateLimit, 'product_id'));
                $idSortRelateLimit = array_values(array_diff($idSortRelateLimit,$pida));
                $idSortRelateIds = array_values(array_slice($idSortRelateLimit, 0, $recommendManualListMaxNumber));
            }
            return ['idSortRelateIds' => $idSortRelateIds, 'idSortRecommendContact' => $idSortRecommendContact];
        });

        if(get('test',0) == 1){
            info(implode(',', $sortPida).'手动推荐ID:'.implode(',', $idSortRelateData['idSortRelateIds']));
            info('手动推荐关系:'.json_encode($idSortRelateData['idSortRecommendContact']));
        }
        return ['idSortRelateIds' => $idSortRelateData['idSortRelateIds'], 'recommendMatomoIds' => $recommendMatomoIds,'idSortRecommendContact' => $idSortRelateData['idSortRecommendContact']];
    }

    /**
     * 获取 matomo 推荐推荐产品 ID
     *
     * @param array $pids  产品ID集合
     * @param int   $limit 限制数量，小于等于 0 表示不限制
     * @param array{cacheable:bool, mode:int, threshold:float, domain:string, site_id:int} $options
     *
     * @return array|array[]
     */
    private static function buildMatomoRecommendProductsId(array $pids, int $limit = 0, array $options = []): array
    {
        // 推荐的模式
        $mode = (int)($options['mode'] ?? Settings::fetch('matomo_recommend_result_mode') ?: 1);
        $isMatomoSource = in_array($mode, [1, 2]);

        //region 从 Redis 中获取已经缓存过的产品关联
        $allows = (array)($options['allows'] ?? Helper::split((string)Settings::fetch('recommend_matomo_allows')));
        $allows = array_map('intval', $allows);
        // 如果 allows 不为空，那就和要查询的产品取交集
        if (!empty($allows)) {
            $pids = array_intersect($pids, $allows);
        }

        $ordersLimit = (int)($options['orders_limit'] ?? Settings::fetch('recommend_matomo_min_orders_limit'));
        if ($ordersLimit > 0) {
            // 查询订单量（依赖 Redis）
            $productRealSales = self::getProductRealSales($pids, 15, 'placed');
            $pids = array_keys(
                array_filter($productRealSales, static function ($sales) use ($ordersLimit) {
                    return $sales >= $ordersLimit;
                })
            );
            $pids = array_map('intval', $pids);
        }

        // 如果空
        if (empty($pids)) {
            return [];
        }

        // 是否允许缓存
        $cacheable = (bool)($options['cacheable'] ?? Settings::fetch('enabled_matomo_recommend_products_cache'));
        // 相似度的阈值
        $threshold = (float)($options['threshold'] ?? Settings::fetch('matomo_recommend_similarity_threshold') ?: 0);
        $scale = (int)1e4;
        $threshold = (int)($threshold * $scale);

        $useScore = in_array($mode, [1, 2, 3], true);

        $aggs = $isMatomoSource && count($pids) > 1;
        $cacheable = $cacheable && !$aggs;
        $cachedProductsId = [];
        $cachedProductsIdWithScore = [];
        if ($cacheable) {
            /** @var Client $redis */
            $redis = RedisService::getRedisDefaultClient();
            $prefix = \Cache::getPrefix();
            $isAll = $limit <= 0;
            foreach ($pids as $id) {
                $cached = [];
                $cacheKey = $prefix . 'matomo-recommend:' . $mode . ':' . $id;
                if (!$redis->exists($cacheKey)) {
                    continue;
                }

                if ($useScore) {
                    $cacheOptions = $isAll ? [] : ['limit' => [0, $limit]];
                    $cacheOptions['withscores'] = true;
                    $cached = $redis->zrevrangebyscore($cacheKey, '+inf', $threshold, $cacheOptions);

                    // 这个 NULL 只是用来占位的，不需要出现在结果中
                    unset($cached['NULL']);
                }

                // 只要缓存的 key 存在，那就判断有效，即使为空就是空数组
                $cachedProductsId[$id] = array_map('intval', array_keys($cached)) ?: [];
                // 值为分数、键为产品ID
                $cachedProductsIdWithScore[$id] = array_map('intval', $cached);
            }
            //endregion

            // 计算一下，从 redis 取了之后是否还有需要拿去的
            $pids = array_diff($pids, array_keys($cachedProductsId));
            if (empty($pids)) {
                return $cachedProductsId;
            }
        }

        $pids = $diff = array_map('intval', $pids);
        $scoreFilter = static function ($productsIdWithScoreGroups, $threshold) {
            $result = [];
            foreach ($productsIdWithScoreGroups as $owner => $groups) {
                $owner = (int)$owner;
                $filtered = [];
                foreach ($groups as $pid => $score) {
                    $pid = (int)$pid;
                    if ($pid !== $owner && $score >= $threshold) {
                        $filtered[] = $pid;
                    }
                }
                $result[$owner] = $filtered;
            }

            return $result;
        };
        $productsIdWithScore = [];
        $matomoProductsId = [];

        if ($isMatomoSource) {
            //region 从 Matomo 接口获取产品推荐
            $domain = $options['domain'] ?? TraceCode::fetch('matomo_tracker_url');
            $siteId = (int)($options['site_id'] ?? TraceCode::fetch('matomo_site_id'));
            $payload = [
                'matomo_domain'  => $domain,
                'matomo_site_id' => $siteId,
                'pid'            => $sentPids = implode(',', $pids),
            ];

            if ($limit > 0) {
                $payload['qty'] = $limit;
            }

            $address = (string)Settings::fetch('matomo_recommend_remote_address') ?: '172.31.54.111';
            $port = (int)Settings::fetch('matomo_recommend_remote_port') ?: '29090';
            $sendTimeout = (int)Settings::fetch('matomo_recommend_send_timeout') ?: 100000;
            $receiveTimeout = (int)Settings::fetch('matomo_recommend_receive_timeout') ?: 10000;

            $server = new MartClient(
                $address, $port,
                ['send_timeout' => $sendTimeout, 'receive_timeout' => $receiveTimeout]
            );


            $response = Helper::requestMartDB(
                $payload,
                'RecommendRelatedProductsHandler',
                'getRecommendRelatedProductsResult',
                $server
            );

            $response = json_decode($response, true) ?: [];
            $productsId = array_filter(
                array_map('intval', explode(',', $response['rec_result']['cf'] ?? '')),
                static function ($id) {
                    return $id > 0;
                }
            );

            if (empty($productsId)) {
                return [];
            }

            if ($aggs) {
                return [$productsId];
            }

            if (count($pids) === 1) {
                $firstPid = current($pids);
                $matomoProductsId = [$firstPid => $productsId];
                $withScore = [];
                $i = count($productsId);
                $baseScore = 99999 * $scale;
                foreach ($productsId as $pid) {
                    $withScore[$pid] = --$i * 1000 + $baseScore;
                }
                $productsIdWithScore = [$firstPid => $withScore];
            } else {
                return [];
            }
        } elseif ($mode === 3) {
            $client = new \GuzzleHttp\Client(['verify' => false, 'timeout' => 2]);
            $dbName = $options['db_source'] ?? config('database.connections.mysql.database');
            $url = sprintf(
                'https://a.nebular.ink:1233//relateApi/relateProduct/cart?db=%s&pids=%s',
                $dbName,
                implode(',', $pids)
            );
            try {
                $responseRaw = $client->get($url)->getBody()->getContents();
                $response = json_decode($responseRaw, true);

                if (isset($_GET['matomo_debug'])) {
                    info(
                        json_encode([
                            'action'   => '采集平台',
                            'request'  => $url,
                            'response' => $response,
                        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES)
                    );
                }

                assert(((int)($response['c'] ?? -1)) === 0, 'HTTP Response: ' . $responseRaw);

                $productsIdWithScore = array_map(static function ($item) use ($scale) {
                    return array_map(static function ($score) use ($scale) {
                        return (int)($score * $scale);
                    }, array_column($item, 'score', 'id'));
                }, array_column($response['data'] ?? [], 'relate', 'id'));

                $matomoProductsId = array_map(static function ($item) {
                    return array_keys($item);
                }, $productsIdWithScore);
            } catch (\Throwable $e) {
                info('请求采集平台出错' . $e);
                $matomoProductsId = [];
            }
        }
        if (empty($matomoProductsId)) {
            return [];
        }

        //endregion

        // 如果不允许缓存或者是聚合查询的，那就直接返回从 matomo 查询到的
        if (!$cacheable) {
            return $scoreFilter($productsIdWithScore, $threshold);
        }

        $productsId = $matomoProductsId;


        //region 把从 matomo 获取到的写入缓存
        $redis->pipeline(
            function (Pipeline $pipeline) use ($useScore, $mode, $diff, $prefix, $productsId, $productsIdWithScore) {
                foreach ($productsId as $id => $relations) {
                    $id = (int)$id;
                    if ($id <= 0 || !in_array($id, $diff, true)) {
                        continue;
                    }
                    $cacheKey = $prefix . 'matomo-recommend:' . $mode . ':' . $id;
                    if ($useScore) {
                        $relationsWithScore = array_only($productsIdWithScore[$id] ?? [], $relations);
                        // 删除掉自身
                        unset($relationsWithScore[$id]);

                        // 即使这个产品没有推荐产品，也使用一个空的 KEY 占据，使其缓存有效
                        if (empty($relationsWithScore)) {
                            $relationsWithScore['NULL'] = 0;
                        }

                        // 加入缓存
                        $pipeline->zadd($cacheKey, $relationsWithScore);
                    }
                    // 缓存 24 小时
                    $pipeline->expire($cacheKey, 86400);
                }
            }
        );
        //endregion

        // 将缓存命中的一并计入查询
        if (!empty($cachedProductsIdWithScore)) {
            $productsIdWithScore = array_replace($cachedProductsIdWithScore, $productsIdWithScore);
        }
        // 这里筛选分值
        return $scoreFilter($productsIdWithScore, $threshold);
    }
}
