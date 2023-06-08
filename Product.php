<?php

namespace Jason\Ccshop\Models;

use App;
use Event;
use Intervention\Image\ImageManagerStatic;
use InvalidArgumentException;
use Jason\Ccshop\Services\MoresiteService;
use Jason\Ccshop\Services\MultiSceneProductService;
use Jason\Ccshop\Services\OrderService;
use Jason\Ccshop\Services\ProductService;
use Jason\Ccshop\Services\PromotionService;
use Jason\Ccshop\Services\RedisService;
use Jason\Ccshop\Services\SceneService;
use Jason\Ccshop\Services\SystemRevisionService;
use Jason\Ccshop\Services\WishlistService;
use Storage;
use BackendAuth;
use Cache;
use Illuminate\Support\Facades\Redis;
use Carbon\Carbon;
use DB;
use Jason\Ccshop\Classes\Helper;
use Jason\Ccshop\Classes\Jobs;
use Jason\Ccshop\Classes\OperateLog;
use Jason\Ccshop\Controllers\FeatureGroups;
use Jason\Ccshop\Controllers\Features;
use Jason\Ccshop\Controllers\Options;
use Jason\Ccshop\Controllers\ProductGroups;
use Jason\Ccshop\Controllers\Promotions;
use Jason\Ccshop\Controllers\Reviews;
use Jason\Ccshop\Controllers\Searches;
use Jason\Ccshop\Controllers\ShopBase;
use League\Flysystem\Exception;
use Log;
use Model;
use RainLab\Translate\Models\Locale;
use RainLab\User\Components\Account;
use Rainlab\User\Models\UserGroup;
use \October\Rain\Database\Traits;
use RainLab\Translate\Classes\Translator;
use File as FileHelper;
use Jason\Ccshop\Classes\BuildFeedProToEs;
use Jason\Ccshop\Models\ProductPresell;
use Jason\Ccshop\Controllers\Products;
use System\Models\File;
use Throwable;
use Jason\Ccshop\Classes\DingTalk\DingTalkClient;
use Jason\Ccshop\Jobs\SyncProductSourceToErp;

/**
 * Products Model
 */
class Product extends Model
{

    public const SIMILAR_IN_STOCK_STATUS = ['instock', 'stockout', 'assessment'];
    public const ONLY_IN_STOCK_STATUS    = 'instock';
    public $implement    = ['RainLab.Translate.Behaviors.TranslatableModel'];
    public $translatable = ['name', 'name2', 'page_title', 'meta_keywords', 'meta_description', 'content'];

    public static $productStatusOptions = [
        "instock", "stocktension", "stockout", "unpublished", "disabled", "untreated", "shelve", "assessment","avlremind"
    ];

    public static $relationNames = [
        'tags',
        'categories',
        'thumb',
        'wishlist',
        'feature_image',
        'point_prices',
        'reviews',
        'features.feature',
        'features.feature_value',
        'private_options.option_values.thumb',
        'private_options.option_values.feature_image',
        'groups.group.items.product.thumb',
        'groups.group.items.product.feature_image',
        'groups.group.feature',
        'product_sizes',
        'product_original_images',
        'product_label',
        'collocation_items'
    ];

    use Traits\Validation;
    use \October\Rain\Database\Traits\Revisionable;

    protected $revisionable = ['name', 'slug', 'status', 'popular', 'list_price', 'price', 'content', 'mobile_content', 'meta_keywords', 'meta_description', 'product_source', 'sort', 'amount', 'spu', 'pid','preferential_status','preferential_price','prefer_product_id'];

    /**
     * @var string The database table used by the model.
     */
    public $table = 'cc_products';

    //固定连接不能重复
    public $customMessages = [
        'unique' => '重复采集',
    ];

    public $rules = [
        'name'       => 'required|between:1,255',
        'name2'      => 'between:1,255',
        'slug'       => 'required|alpha_dash|between:1,255',
        'status'     => 'required',
        'rating'     => 'numeric|min:1|max:5',
        'price'      => 'required|numeric|digits_between:1,14',
        'categories' => 'array',
    ];

    public $jsonable = ['product_source','promo_info'];
    /**
     *
     * @var array Relations
     */
    public $hasOne = [
        'point_prices' => ['Jason\Ccshop\Models\ProductPointPrice', 'key' => 'product_id'],
        'presell' => ['Jason\Ccshop\Models\ProductPresell'],
        'product_sizes' => ['Jason\Ccshop\Models\ProductSize','key' => 'product_id'],
        'product_original_images' => ['Jason\Ccshop\Models\ProductOriginalImages','key' => 'product_id'],
        'product_additional_info' => ['Jason\Ccshop\Models\ProductAdditionalInfo','key' => 'product_id']
    ];
    public $hasMany = [
        'prices'            => ['Jason\Ccshop\Models\ProductPrices', 'conditions' => 'lower_limit > 1'],
        'shoppingcartvalue' => ['Jason\Ccshop\Models\ShoppingCartValue'],
        'order_products'    => ['Jason\Ccshop\Models\OrderProduct'],
        'features'          => ['Jason\Ccshop\Models\ProductToFeature'],
        'feature_value'     => ['Jason\Ccshop\Models\ProductToFeature'],
        'private_options'   => ['Jason\Ccshop\Models\Option', 'key' => 'product_id', 'order' => 'sort asc'],
        'galleries'         => ['Jason\Ccshop\Models\OptionGallery'],
        'groups'            => ['Jason\Ccshop\Models\ProductGroupItem'],
        'wishlist'          => ['Jason\Ccshop\Models\Wishlist', 'key' => 'product_id'],
        'reviews'           => ['Jason\Ccshop\Models\Review', 'key' => 'product_id', 'conditions' => 'audit = 1','scope'=>'Scene'],
        'skus'     => ['Jason\Ccshop\Models\ProductSku','key' => 'product_id'],
        'related'  => ['Jason\Ccshop\Models\ProductRelate', 'key' => 'product_id'],
        'instock_reason'    => ['Jason\Ccshop\Models\InstockReason', 'key' => 'product_id'],
        'shelve_reason'    => ['Jason\Ccshop\Models\ShelveReason', 'key' => 'product_id'],
        'scene_products'    => ['Jason\Ccshop\Models\ProductScene'],
        'product_group_sku' => ['Jason\Ccshop\Models\GroupProductSku', 'key' => 'group_product_id'],
        'relateds'  => ['Jason\Ccshop\Models\ProductRelate', 'key' => 'related_id'],
    ];
    public $belongsTo = [
        'admin'                   => ['Backend\Models\User', 'key' => 'admin_id', 'scope' => 'withTrashed'],
        'update_admin'            => ['Backend\Models\User', 'key' => 'update_id', 'scope' => 'withTrashed'],
        'chinese_editor'          => ['Backend\Models\User', 'key' => 'chinese_editor_id', 'scope' => 'withTrashed'],
        'foreign_language_editor' => ['Backend\Models\User', 'key' => 'foreign_language_editor_id', 'scope' => 'withTrashed'],
        'brand'                   => ['Jason\Ccshop\Models\ProductBrand', 'key' => 'brand_id'],
    ];
    public $belongsToMany = [
        "categories"            => [
            'Jason\Ccshop\Models\Category',
            'table' => 'cc_product_categories',
        ],
        'features_entity'       => [
            'Jason\Ccshop\Models\Feature',
            'table' => 'cc_product_to_features',
        ],
        'feature_values_entity' => [
            'Jason\Ccshop\Models\FeatureValue',
            'table' => 'cc_product_to_features',
        ],
        "options"               => [
            'Jason\Ccshop\Models\Option',
            'table'      => 'cc_product_to_options',
            'key'        => 'product_id',
            'otherKey'   => 'option_id',
            'conditions' => 'cc_product_options.product_id = 0',
        ],
        "custom_options"        => [
            'Jason\Ccshop\Models\CustomOption',
            'table'      => 'cc_product_to_custom_options',
            'key'        => 'product_id',
            'otherKey'   => 'custom_option_id',
            'conditions' => 'parent_id = 0',
        ],
        'bind_sections'         => [
            'Jason\Ccshop\Models\ProductSection',
            'key'   => 'product_id',
            'table' => 'cc_product_section_to_products',
        ],
        'selected_sections'     => [
            'Jason\Ccshop\Models\ProductSection',
            'key'   => 'product_id',
            'table' => 'cc_product_section_selected',
        ],
        'product_related' => [
            'Jason\Ccshop\Models\Product',
            'key' => 'related_id',
            'table' => 'cc_product_related_to_products'
        ],
        'product_chain_related' => [
            'Jason\Ccshop\Models\Product',
            'key' => 'product_id',
            'otherKey' => 'related_id',
            'table' => 'cc_product_chain_related'
        ],
        'product_group' => [
            'Jason\Ccshop\Models\Product',
            'key' => 'group_product_id',
            'otherKey' => 'item_product_id',
            'table' => 'cc_group_product'
        ],
        'orders' => [
            'Jason\Ccshop\Models\Order',
            'table' => 'cc_order_products',
            'key' => 'product_id',
            'otherKey' => 'order_id'
        ],
        'collocation_items'     => [
            'Jason\Ccshop\Models\CollocationItems',
            'key'   => 'product_id',
            'otherKey'   => 'collocation_items_id',
            'table' => 'cc_collocation_items_products',
        ],
        'tag_icons' => [
            'Jason\Ccshop\Models\TagIcon',
            'table' => 'cc_product_to_tag_icons',
        ],
        'product_label' => ['Jason\Ccshop\Models\Label',
            'key' => 'product_id',
            'table' => 'cc_label_to_products'
        ],
        'new_tags' => [
            'Jason\Ccshop\Models\Tag',
            'key' => 'product_id',
            'table' => 'cc_products_to_tags',
        ],
        'business' => [
            'Jason\Ccshop\Models\Business',
            'table' => 'cc_business_product',
            'key' => 'product_id',
            'otherKey' => 'business_id'
        ]
    ];
    public $morphTo   = [];
    public $morphOne  = [];
    public $morphMany = [
        'reward_point'     => ['Jason\Ccshop\Models\RewardPoint', 'name' => 'bind'],
        'use_point'     => ['Jason\Ccshop\Models\UsePoint', 'name' => 'bind'],
        'rebate'           => ['Jason\Ccshop\Models\Rebate', 'name' => 'bind'],
        'custom_fields'    => ['Jason\Ccshop\Models\CustomField', 'name' => 'bind'],
        'revision_history' => ['System\Models\Revision', 'name' => 'revisionable'],
    ];
    public $morphToMany = [
        'tags' => ['Jason\Ccshop\Models\Tag', 'table' => 'cc_tag_gables', 'name' => 'taggable'],
    ];
    public $attachOne  = [
        'y_thumb' => ['Jason\Ccshop\Models\BaseFile'],
    ];
    public $attachMany = [
        'thumb'         => ['Jason\Ccshop\Models\BaseFile'],
        'feature_image' => ['Jason\Ccshop\Models\BaseFile'],
        'media'         => ['Jason\Ccshop\Models\BaseFile'],
        'media_image'   => ['Jason\Ccshop\Models\BaseFile'],
        'content_image' => ['Jason\Ccshop\Models\BaseFile'],
        'spare_image'   => ['Jason\Ccshop\Models\BaseFile'],
        'content_media'         => ['Jason\Ccshop\Models\BaseFile'],
        'content_media_image'   => ['Jason\Ccshop\Models\BaseFile'],
    ];
    /**
     *
     * @var array Guarded fields
     */
    protected $guarded = ['*'];
    /**
     *
     * @var array Fillable fields
     */
    protected $fillable = [
        'id', 'code', 'name', 'name2', 'slug', 'status', 'admin_id', 'update_id',
        'list_price', 'price', 'amount', 'min_qty', 'max_qty', 'saled',
        'page_title', 'meta_keywords', 'meta_description', 'content', 'sort',
        'code_content', 'product_source', 'remark', 'popular', 'supply', 'weight',
        'supply_product','spu','spu_status','purchase_list_price','purchase_price',
        'shopkeeper_id','category_id', 'pid','priority_user','confirm_in_stock_at',
        'pl_updated_at','recommend_user', 'goods_no'
    ];

    protected $casts = [
        'price'      => 'float',
        'list_price' => 'float',
    ];

    //推送产品操作记录用户
    static $backendUserId = 0;

    public function getDropdownOptions($fieldName = null)
    {
        switch ($fieldName) {
            case 'status':
                return array_combine(self::$productStatusOptions, self::$productStatusOptions);
                break;
            case 'rating':
                return [5 => 5, 4 => 4, 3 => 3, 2 => 2, 1 => 1];
                break;
        }
        return [];
    }

    public function getSaledCount()
    {
        return Cache::tags('product_saled_count')->remember($this->id, 120, function () {
            //商品下单量改为30天的下单量
            $thirtyDayAgo = date('Y-m-d', strtotime('-30 day'));
            // 缓存指定日期开始的首个订单 ID
            $orderId = Cache::tags('product_saled_count')->remember('date:' . $thirtyDayAgo, 1440,
                function () use ($thirtyDayAgo) {
                    return Order::where('created_at', '>', $thirtyDayAgo)
                            ->select('id')
                            // 请注意，first 可能会返回 null ，从而导致 remember 不能缓存它，
                            // 所以，需要返回一个非 null 值的空值，让他把 null 也缓存了
                            ->first()->id ?? 0;
                });
            // 如果为空就返回了
            if (empty($orderId)) {
                return 0;
            }

            $saledCount = OrderProduct::where('product_id', $this->id)
                ->cacheTags('product_saled_count')
                // 只是缓存 10 分钟就行
                ->remember(10)
                ->where('order_id', '>', $orderId)
                ->sum('qty');
            return intval($saledCount);
        });
    }

    public function getStatusLabelClass($status)
    {
        switch ($status) {
            case 'instock':
                $class = "success";
                break;
            case 'stockout':
                $class = "warning";
                break;
            case 'unpublished':
                $class = "danger";
                break;
            case 'disabled':
                $class = "default";
                break;
            case 'shelve':
                $class = "danger";
                break;
            case 'untreated':
                $class = "default";
                break;
            case 'stocktension':
                $class = "info";
                break;
            case 'assessment':
                $class = "info";
                break;
            case 'avlremind':
                $class = "primary";
                break;
            default:
                $class = 'default';
                break;
        }

        return $class;
    }

    public function getMainThumb($width = 50, $height = 'default', $options = [], $offset = 0)
    {   
        $origin = false;
        $images = $this->getImageCollection();
        if ($images->isEmpty() || empty($images[$offset])) {
            return ShopBase::getPluginAssetsPath() . "images/default-thumb.png";
        }

        $options = !empty($options) ? $options : ['mode' => "auto", 'quality' => 100];
        $height == 'default' && $height = $width;

        if (is_array($images[$offset])) {
            return $images[$offset]['path'];
        }

        if ($width == 'origin' || $origin) {
            $path = $images[$offset]->getPath();
        } else {
            $path = $images[$offset]->getThumb($width, $height, $options);
        }

        return Event::fire('jason.ccshop.getThumb', [$path, 'thumb'], true);
    }

    /**
     * 根据场景ID获取图片
     * @param $width
     * @param $height
     * @param $sceneId
     * @return string
     */
    public function getMainThumbBySceneId($width = 50, $height = 'default',$sceneId = 0)
    {
        $images = $this->filterRelationProductData('feature_image',$sceneId)->merge(
            $this->filterRelationProductData('thumb',$sceneId)
        );

        // 场景如果数据为空, 获取默认产品数据
        if ($images->isEmpty()) {
            return $this->getMainThumb($width, $height);
        }

        $height == 'default' && $height = $width;
        $offset = 0;
        if (!empty($images[$offset])) {
            if (is_array($images[$offset])) {
                return $images[$offset]['path'];
            }

            $path = $images[$offset]->getThumb($width, $height);

            return Event::fire('jason.ccshop.getThumb', [$path, 'thumb'], true);
        }

        return ShopBase::getPluginAssetsPath() . "images/default-thumb.png";
    }

    public function inCategories(array $desCids)
    {
        $cids = $this->getProductCids();
        if (empty($cids)) {
            return false;
        }
        $tmp = array_intersect($cids, $desCids);
        return !empty($tmp) ? true : false;
    }

    public static function getCategoryWithDepth($pid, $depth = -1)
    {
        $categories = Category::whereHas('products', function ($q) use ($pid) {
            $q->where('id', $pid);
        })->get();
        if ($depth < 0) {
            $maxDepth = $categories->max('nest_depth');
            $depth    = $maxDepth + 1 + $depth;
        } else {
            $depth = intval($depth);
        }
        return $categories->first(function ($key, $value) use ($depth) {
            return intval($value->nest_depth) === $depth;
        });
    }

    public function getProductCids()
    {
        $categories = $this->categories;
        if (is_object($categories)) {
            $categories = $categories->toArray();
        }

        if (count($categories) == 0 || !is_array($categories)) {
            return [];
        }

        $cids = [];
        foreach ($categories as $category) {
            $cids[] = $category['id'];
        }

        return $cids;
    }

    public function getFeatureValuesId($featureIds)
    {
        if (empty($featureIds) || empty($this->feature_value)) {
            return false;
        }

        if (!is_array($featureIds)) {
            $featureIds = [$featureIds];
        }

        $ids = [];
        foreach ($this->feature_value as $value) {
            if (in_array($value->feature_id, $featureIds)) {
                $ids[] = $value->feature_value_id;
            }
        }
        return $ids;
    }

    /**
     * 计算产品价格
     * @param  array  $labels 引用传入促销标签
     *
     * @return array
     */
    public function sellingPrice(&$labels = [])
    {  
        SceneService::setTempSceneId(0);
        $data = Promotions::productSellingPrice($this,'detail',$labels);
        SceneService::clearTempSceneId();
        return $data;
    }

    public static function getProductField($pid, $field)
    {
        if (empty($pid)) {
            return false;
        }
        return self::query()->where("id", $pid)->pluck($field);
    }

    /**
     * @param $key
     * @param null $fields
     * @param bool $instock
     * @return boolean/Prduct
     */
    public static function getProduct($key, $fields = null, $instock = false)
    {
        $query     = self::query();
        if (!empty($fields)) {
            $query->select($fields);
        }
        if (is_array($key)) {
            $query->where($key);
        } elseif (is_numeric($key) || is_string($key)) {
            $query->where('id', $key)->orWhere('slug', $key);
        } else {
            return false;
        }

        if ($instock === true) {
            $query->InStock();
        }
        if (get('preview') === 'true') {
            $product = $query->first();
        } else {
            $product = $query->cacheTags('products')->remember(1440)->first();
        }
        if (empty($product)) {
            return false;
        }
        return $product;
    }

    public static function getProductFeaturesTree($pid)
    {
        return Cache::tags('product-features-tree')
            ->remember($pid, 1440, function () use ($pid) {
                $cacheTags = ['product-to-features'];
                $items     = ProductToFeature::where('product_id', $pid)->cacheTags($cacheTags)->remember(1440)->get();
                if (count($items) == 0) {
                    return [];
                }

                $data = [];
                foreach ($items as $item) {
                    if (empty($data[$item->feature_id])) {
                        $data[$item->feature_id] = [];
                    }
                    if (in_array($item->feature_value_id, $data[$item->feature_id])) {
                        continue;
                    }
                    $data[$item->feature_id][] = $item->feature_value_id;
                }
                return $data;
            });
    }

    /**
     * 获取产品所有选项的笛卡尔积矩阵
     *
     * @return array
     */
    public function optionsCartesian()
    {
        $allOptions = [];

        // 构造产品所有选项的ID分组结构
        foreach ($this->private_options as $option) {
            $allOptions[$option->id] = [];
            foreach ($option->option_values as $value) {
                $allOptions[$option->id][] = $value->id;
            }
        }

        // 生成所有选项组合的笛卡尔积矩阵
        $allOptionsCartesian = Helper::cartesian($allOptions);

        // 按照选项ID重新排序，避免可能会发生的选项顺序不一致
        ksort($allOptionsCartesian);

        // 重新索引数组，使下标从0开始
        $allOptionsCartesian = array_merge($allOptionsCartesian);

        return $allOptionsCartesian;
    }

    public static function packageProductFeatureGroup(&$product)
    {
        $query = ProductToFeature::where("product_id", $product->id);
        ShopBase::withFactory($query, ['feature', 'feature_value'], 'products');
        $features = $query->cacheTags('products')->remember(1440)->get();
//        $features = $product->features;
        if (count($features) == 0) {
            return false;
        }

        $groups = [];
        foreach ($features as $feature) {
            $group = FeatureGroups::getGroup($feature->feature->group_id);
            if ($feature->feature && count($group)) {
                if (!isset($groups[$group->id])) {
                    $groups[$group->id] = $group->toArray();
                }
                $groups[$group->id]['features'][] = $feature;
            }
        }
        $product->feature_groups = $groups;
        return true;
    }

    public function customField($key)
    {
        $cacheKey = md5(md5($this) . md5($key));
        return \Cache::tags(['custom-fields', 'custom-field-' . $key])->remember($cacheKey, 1440, function () use ($key) {
            return CustomField::getValue($this, $key);
        });
    }

    public function increaseWishTotal($count = 1)
    {
        $count = (int)$count;
        $wishField = CustomField::findOne($this->id, get_class(), 'wish_total');
        $value     = !empty($wishField->value) ? intval($wishField->value) + $count : 1;
        CustomField::setValue($this, 'wish_total', $value);

        $redisCli = RedisService::getRedisStoreClient();
        $cacheKey = WishlistService::getWishTotalCacheKey();

        if ($redisCli->hexists($cacheKey, $this->id)) {
            // 自增/自减
            $redisCli->hincrby($cacheKey, $this->id, $count);
        } else {
            $redisCli->hset($cacheKey, $this->id, (int)$value);
        }

        //给产品汇总表更新收藏数（石小梅）
        \Jason\Ccshop\Models\ProductStatistic::saveStatInfo(['product_id' => $this->id], ['collection' => $value]);
    }

    public function dumpCache($options = [])
    {
        $host = !empty($options['host']) ? $options['host'] : null;
        $esIndexName = !empty($options['index']) ? $options['index'] : null;

        $redisLock = RedisService::getRedisStoreClient();
        $prefix  = config('cache.prefix', 'ccshop');
        $hashKey =  $prefix.':hash:products-dumpCount';

        $arr = $this->dumpArray();

        $arr['list_price'] = $this->getOriginal('list_price');

        $discount_judge = (bool)Settings::fetch('google_feed_discount_judge', false);
        if(!$discount_judge){
            $arr['src_price'] = $this->getOriginal('price');
        }

        $es          = Searches::esInstance($host);
        $langs       = Locale::listEnabled();
        if (empty($esIndexName)) {
            $esIndexName = Searches::getSearchEngineIndexName();
        }

        foreach ($langs as $code => $lang) {
            $cacheData     = $arr;
            $indexFullName = $esIndexName . '-' . $code;
            Searches::checkOrCreateIndex($indexFullName, $es);

            foreach ($this->translatable as $field) {
                $cacheData[$field] = $this->lang($code)->$field;
            }

            if (!empty($this->categories)) {
                foreach ($this->categories as $key => $category) {
                    foreach ($category->translatable as $f) {
                        $cacheData['categories'][$key][$f] = $this->categories[$key]->lang($code)->$f;
                    }
                }
            }

            $params = [
                'index' => $indexFullName,
                'type'  => 'products',
                'id'    => $this->id,
                'body'  => $cacheData
            ];

            try {

                $res = $es->index($params);

                if (empty($res['_shards']['successful'])) {
                    info("产品ES索引创建异常，产品ID：".$this->id."，es返回信息：".PHP_EOL.json_encode($res));
                    throw new \Exception("产品ES索引创建异常");
                }

                if (in_array($this->status, ProductService::$instockStatus)) {
                    // 重建产品缓存数据
                    ProductService::rebuildProductCacheFromData($cacheData);
                    // 重建场景缓存数据
                    MultiSceneProductService::rebuildProductSceneCacheFromData($this->id, $arr['scene_data']);

                    // 后台启用新搜索 或者 命令行下执行feed es
                    if ((Settings::fetch('enabled_search_v2_algorithm') && \App::runningInBackend()) || \App::runningInConsole()) {
                        // 构建Feed产品ES
                        $feedProBuilder = app(BuildFeedProToEs::class);
                        $feedProBuilder->setProductData($cacheData)->writeToEs();
                    }
                }

                // 清除前台产品详情页redis缓存
                self::clearCache($this->id);
                $redisLock->hdel($hashKey, $this->id); //更新成功,删除更新次数

            } catch (\Exception $e) {

                $this->setProduceDumpCouunt($redisLock,$this->id,$e);
                info("产品ES索引创建异常，产品ID：".$this->id."，产品数据：".PHP_EOL.json_encode($params).PHP_EOL.
                    "报错信息：".PHP_EOL.$e->getMessage().PHP_EOL.$e->getTraceAsString());
                throw $e;
            }
        }

        $arr['list_price'] = $this->list_price;
        $arr['src_price'] = $this->src_price;

        return $arr;
    }


    /**
     * @param $redisLock
     * @param $id
     * @param $e
     * 设置更新所有失败次数
     */
    public function setProduceDumpCouunt($redisLock, $id ,$e)
    {
        $prefix  = config('cache.prefix', 'ccshop');
        $hashKey =  $prefix.':hash:products-dumpCount';
        $hashProductsCount = $redisLock->hmget($hashKey, $id);  //获取更新次数
        $hashProductsCount = $hashProductsCount[0] ? $hashProductsCount[0] : 0;
        if ($hashProductsCount >= 2) {

            $message =  '更新新品索引失败'.($hashProductsCount + 1).'次，请求的url是 ' . request()->url() . '，' . "\r\n"
                . '产品id是 ' . $this->id . "\r\n"
                . '错误信息是 --- ' . PHP_EOL . $e->getMessage() . PHP_EOL . $e->getTraceAsString();
            info($message);
            $accessToken = config('app.exception_dingtalk_access_token');
            $secret = config('app.exception_dingtalk_secret');
            if($accessToken && $secret){
                $dingTalkClient = DingTalkClient::client($accessToken, $secret);
                $dingTalkMessage = $dingTalkClient->makeTextMessage();
                $dingTalkMessage->setContent($message);
                $dingTalkClient->send();
            }
        }
        $hashData = [$id => ($hashProductsCount + 1)];
        $redisLock->hmset($hashKey, $hashData);

    }

    /**
     * 获取压缩后的图片路径
     * @param $id
     * @return null
     */
    public function getMainYhumb($id){
        $ythumb = \DB::table('cc_product_y_thumb')->where('product_id',$id)->pluck('y_thumb');
        if($ythumb){
            return $ythumb;
        }else{
            return null;
        }
    }

    public function dumpArray($loaded = false)
    {
        if ($loaded === false) {
            $this->load(self::$relationNames);
        }
        $w                              = Settings::fetch('product_list_thumb_width', 344);
        $h                              = Settings::fetch('product_list_thumb_height', 420);
        $mode                           = Settings::fetch('thumb_mode', 'auto');
        $quality                        = Settings::fetch('thumb_quality', 95);
        $options                        = ['mode' => $mode, 'quality' => $quality];
        $this->f_thumb                  = $this->getFThumb($w, $h, $options);
        // 目前改字段(f_thumb_webp)暂时不用, 先注释
        // $this->f_thumb_webp             = $this->getFThumb($w, $h, $options, 0, true);
        $this->s_thumb                  = $this->getMainThumb($w, $h, $options, 1);
        // 这是原来的逻辑
        $this->feed_thumb               = $this->getMainThumb(602, 602, ['quality' => 100]) ?: '';
        // 这是 Facebook 专属的，如果启用这个开关，在取的时候就要取这个字段，但是为了保证可用性，在保存时不限制它
        $this->facebook_feed_thumb      = $this->getFacebookFeedThumb($this->feed_thumb,
            $this->f_thumb);
        $this->fl_editor = $this->foreign_language_editor?($this->foreign_language_editor->last_name . $this->foreign_language_editor->first_name):"缺省";

        try {
            $featureImages = $this->filterRelationProductData('feature_image')->map(function ($item) {
                return Event::fire('jason.ccshop.getThumb', [$item->getPath(), 'thumb'], true);
            })->toArray();
        } catch (Throwable $exception) {
            $featureImages = [];
        }
        $this->facebook_feed_thumbs     = $this->getFacebookFeedThumbs($featureImages);
        $this->reviews_total            = count($this->reviews);
        $this->wishlist_total           = count($this->wishlist);
        empty($this->url) && $this->url = $this->getUrl();
        if (strpos($this->url, '/-p') === 0) {
            //Jobs::putRedumpPid($this->id);
            $msg = 'URL error:' . $this->url . '. slug: ' . $this->slug . ' stack: ' . json_encode(debug_backtrace());
            file_put_contents(storage_path('temp/url-error-' . $this->id . '.txt'), $msg);
            ShopBase::log('error', 'URL error:' . $this->url . '. slug: ' . $this->slug);
        }
        $this->discount = $this->getDiscount();
        $this->cids     = $this->categories_ids;
        $this->points   = $this->getPointsAttribute();

        // 特征图转换为webp格式
        // 目前暂时不用, 先注释
        // $this->convertFeatureImageToWebp($featureImages);

        empty($this->promotion_price) && $this->promotion_price = $this->sellingPrice()['price'];
        empty($this->src_price) && $this->src_price = $this->price;
        if ($this->promotion_price < $this->list_price) {
            $this->price = $this->promotion_price;
        }

        $sceneData = $this->packageSceneData();
        $arr = $this->toArray();

        $arr['scene_data'] = $sceneData;
        $arr['feature_image'] = $this->filterRelationProductData('feature_image')->toArray();
        $arr['thumb'] = $this->filterRelationProductData('thumb')->toArray();
        $arr['product_main_source_url'] = $this->getMailSourceUrl();
        $arr['product_main_source_md5'] = md5($this->getMailSourceUrl());
        $arr['custom_fields'] = CustomField::getAllFieldValues($this);
        $arr['url']           = $this->getUrl();
        $arr['features']      = Features::getProductFeatures($this, ['scene_id' => 0]);
        $arr['groups']        = ProductGroups::getProductGroups($this);
        $arr['options']       = Options::getProductOptions($this);
        $arr['created_at']    = Helper::timeConvert($arr['created_at']);
        $arr['updated_at']    = Helper::timeConvert($arr['updated_at']);
        $arr['rat_num']       = round(Reviews::getReviewRating($this->id),1);
        $arr['media']         = $this->filterRelationProductData('media')->toArray();

        //获取备用首图
        $spareImages = $this->filterRelationProductData('spare_image')->toArray();
        $spareImgs = [];
        foreach($spareImages as $k=>$img){
           $spareImgs[$k]['path'] = $img['path'];    
        }

        $arr['spare_image'] = $spareImgs;


        //虚拟评论数
        $es_reviews_total     = isset($arr['custom_fields']['reviews_total'])?$arr['custom_fields']['reviews_total']:0;
        //评论数 = 实际评论数 + 虚拟评论数
        $arr['reviews_count'] += (int)$es_reviews_total;
        if (array_key_exists('instock_time',$arr) && $arr['instock_time'])
        {
            $arr['instock_time']  = Helper::timeConvert($arr['instock_time']);
        }

        if (array_key_exists('foreign_language_finished_at',$arr) && $arr['foreign_language_finished_at'])
        {
            $arr['foreign_language_finished_at'] = Helper::timeConvert($arr['foreign_language_finished_at']);
        }

        // chinese_finished_at字段不需要往ES存储
        if (array_key_exists('chinese_finished_at', $arr)) {
            unset($arr['chinese_finished_at']);
        }
        // promo_info字段不需要往ES存储
        if (array_key_exists('promo_info', $arr)) {
            unset($arr['promo_info']);
        }

        $arr['sales']         = $this->getSaledCount();
        if($this->skus){
            $arr['sku'] = $this->skus->toArray();
            $stockpile = $this->getSpuStockpile($arr['sku']);
            $arr['stockpile'] = $stockpile[0];
        }
        //es存储产品预售信息
        if($this->presell){
            $arr['presell'] = $this->presell->toArray();
            $discount = "1";
            if($this->presell->start_time <= Carbon::now() && $this->presell->end_time >= Carbon::now())
            {
                $presellDays = Carbon::parse($this->presell->start_time)->diffInDays(Carbon::parse($this->presell->end_time));
                $interval = floor($presellDays/3);
                if(Carbon::now() >= $this->presell->start_time && Carbon::now() < Carbon::parse($this->presell->start_time)->addDays($interval)){
                    $discount = Settings::fetch('discount1');
                }elseif(Carbon::now() >= Carbon::parse($this->presell->start_time)->addDays($interval) && Carbon::now() < Carbon::parse($this->presell->start_time)->addDays($interval*2)){
                    $discount = Settings::fetch('discount2');
                }else{
                    $discount = Settings::fetch('discount3');
                }
            }
            $arr['presell_discount'] = $discount;
        }
        $app_image_thumb_enable = Settings::fetch('app_image_thumb_enable', 0);
        if($app_image_thumb_enable){
            $arr['thumb_list'] = $this->getFeatureImage();
        }
        unset($arr['foreign_language_editor']);
        unset($arr['private_options']);
        unset($arr['scene_products']);
        unset($arr['remark']);
        return $arr;
    }

    /**
     * 转换特色图格式为webp
     * @var $items
     */
    private function convertFeatureImageToWebp($items)
    {
        $enabledWebp = Settings::fetch('enabled_convert_images_to_webp', false);
        if (count($items) == 0 || ! $enabledWebp) {
            return;
        }

        foreach ($items as $key => $item) {
            $pathWebp = $item->getPathWebp();
            if (! empty($pathWebp)) {
                $item->path_webp = $pathWebp;
            }
        }
    }

    public function getImageCollection()
    {
        return $this->filterRelationProductData('feature_image')->merge($this->filterRelationProductData('thumb'));
    }

    /**
     * 裁剪压缩特征图首图
     * @param $w
     * @param $h
     * @param $options
     * @param int $offset
     * @param bool $isWebp
     * @return |null
     */
    public function getFThumb($w, $h, $options, $offset = 0, $isWebp = false)
    {
        $images = $this->getImageCollection();

        if ($images->isEmpty() || empty($images[$offset])) {
            return ShopBase::getPluginAssetsPath()."images/default-thumb.png";
        }

        $image = $images[$offset];

        return $image->getThumbWithQuality($w, $h, $options, $isWebp);
    }

    /**
     * 图片格式转换
     * 支持jpg,png,gif,web
     *
     * @param $fromPath
     * @param $savePath
     * @param  int  $quality
     * @throws \Exception
     */
    private function convertingImageFormat($fromPath, $savePath, $quality = 75)
    {
        $size = getimagesize($fromPath);

        if (empty($size['mime'])) {
            throw new \Exception("获取图片mime type出错");
        }

        $mime = $size['mime'];

        switch ($mime) {
            case 'image/jpeg':
                $img = @imagecreatefromjpeg($fromPath);
                break;
            case 'image/gif':
                $img = @imagecreatefromgif($fromPath);
                break;
            case 'image/png':
                $img = @imagecreatefrompng($fromPath);
                break;
            case 'image/webp':
                $img = @imagecreatefromwebp($fromPath);
                break;

            default:
                throw new \Exception(sprintf('Invalid mime type: %s. Accepted types: image/jpeg, image/gif, image/png, image/webp',
                    $mime));
                break;
        }

        $pathinfo = pathinfo($savePath);
        if (empty($pathinfo['extension'])) {
            throw new \Exception("获取图片后缀失败");
        }

        $extension = $pathinfo['extension'];

        switch (strtolower($extension)) {
            case 'jpg':
            case 'jpeg':
                // Check JPG support is enabled
                if (imagetypes() & IMG_JPG) {
                    imagejpeg($img, $savePath, $quality);
                }
                break;

            case 'webp':
                if (imagetypes() & IMG_WEBP) {
                    imagewebp($img, $savePath, $quality);
                }
                break;

            case 'gif':
                // Check GIF support is enabled
                if (imagetypes() & IMG_GIF) {
                    imagegif($img, $savePath);
                }
                break;

            case 'png':
                // Scale quality from 0-100 to 0-9
                $scaleQuality = round(($quality / 100) * 9);

                // Invert quality setting as 0 is best, not 9
                $invertScaleQuality = 9 - $scaleQuality;

                // Check PNG support is enabled
                if (imagetypes() & IMG_PNG) {
                    imagepng($img, $savePath, $invertScaleQuality);
                }
                break;

            default:
                throw new \Exception(sprintf('Invalid image type: %s. Accepted types: jpg, gif, png.', $extension));
                break;
        }

        // Remove the resource for the resized image
        imagedestroy($img);
    }

    /**
     * 验证图片链接是否有效
     * @param $url
     * @return bool
     */
    private function verifyImageUrl($url)
    {
        $parseUrl = parse_url($url);
        if (empty($parseUrl['scheme'])) {
            $url = 'https:'.$url;
        }

        $newData = @file_get_contents($url);
        if (! $newData) {
            return false;
        }

        return true;
    }

    public function beforeValidate()
    {
        $customFields = array_pull($this->attributes, 'CustomFields');
        if (!empty($customFields) && !empty($this->id)) {
            foreach ($customFields as $key => $value) {
                $value = $value ?: '';
                CustomField::setValue($this, $key, $value);
            }
        }
    }

    public function beforeSave()
    {
        //为了解决修改记录bug,当价格为整数时,因为数据库存的是2位小数,系统会误认为修改了价格去记录
        $this->price = sprintf("%.2f", $this->price);
        $this->list_price = sprintf("%.2f", $this->list_price);

        if (count($this->product_group) > 0){
            $result = GroupProduct::SaveData();
            if ($result !== true){
                throw new \ValidationException([
                    'group_product' => $result['message']
                ]);
            }
        }
        //在库理由选择
        $is_updata_web = post('is_updata_web', 0);
        $is_open_instock_reason = Settings::fetch('is_open_instock_reason', 0);
        $instock_reason_start_time = Settings::fetch('instock_reason_start_time', '0000-00-00 00:00:00');
        //在修改页面操作的，后台开了在库限制，商品创建时间大于后台设置时间
        if (!empty($is_updata_web) && !empty($is_open_instock_reason) && $this->created_at >= $instock_reason_start_time) {
            $instock_reason = post('instock_reason', []);
            $remark_reason = post('remark_reason', '');
            $countInstockReason = InstockReason::where('product_id', $this->id)->count();
            //从未填过理由，修改状态为在库
            if ($countInstockReason == 0 && $this->status == 'instock') {
                if (count($instock_reason) == 0) {
                    throw new \ValidationException([
                        'published_at' => '请选择在库理由！！！'
                    ]);
                    return false;
                }
                $reason = [];
                $reasonUser = BackendAuth::getUser();
                foreach ($instock_reason as $key => $val) {
                    $reason[$key]['reason'] = $val;
                    $reason[$key]['uid'] = $reasonUser->id;
                    $reason[$key]['product_id'] = $this->id;
                    $reason[$key]['created_at'] = Carbon::now()->toDateTimeString();
                    $reason[$key]['updated_at'] = Carbon::now()->toDateTimeString();
                    if ($val == 0) {
                        if (empty($remark_reason)) {
                            throw new \ValidationException([
                                'published_at' => '请填写其他理由！！！！'
                            ]);
                            return false;
                        }
                        $reason[$key]['remark_reason'] = $remark_reason;
                    }else{
                        $reason[$key]['remark_reason'] = '';
                    }

                }
               InstockReason::insert($reason);
            }
        }

        //判断第一次更改状态为instock的时候记录时间。
        $changeFiled = $this->getDirty();
        //去掉后台手动上传图片时自动设置的宽度
        if (array_key_exists('content', $changeFiled)) {
            $flag = preg_match_all("/<img.*?style=\"width: 300px;\" class=\"fr-fic fr-dib\" data-result=\"success\">/", $changeFiled['content'], $matches);

            if ($flag) {
                foreach ($matches[0] as $match) {
                    $changeFiled['content'] = str_replace($match, str_replace(' style="width: 300px;"', '', $match), $changeFiled['content']);
                }

                if ($this->content) {
                    $this->content = $changeFiled['content'];
                }
            }
            $filterContent = $this->filterContent($this->content);
            $this->content = $filterContent;
        }


        if (array_key_exists('status', $changeFiled) and $changeFiled['status'] == 'instock') {
            if (!$this->instock_time) {
                $this->instock_time = Carbon::now();
            }
            $is_avlremind =  ProductArrivalReminder::where('product_id',$this->id)->where('product_status',0)->update(['product_status'=>1]);
        }
        // 防止站内下架爆款产品, 当产品销量(已付款)超过10,且运营操作下架或者没有库存时, 须填写下架理由
        $reason = post('shelve_reason', '');
        if (
            App::runningInBackend() &&
            array_key_exists('status', $changeFiled) &&
            in_array($changeFiled['status'], ['shelve', 'stockout']) &&
            $this->saledTotal > 10 && empty($reason)
        ) {
            throw new \ValidationException([
                'published_at' => '请填写下架理由！'
            ]);
        }
        $reason && ShelveReason::create([
            'product_id' => $this->id,
            'uid' => data_get(BackendAuth::getUser(), 'id'),
            'reason' => $reason
        ]);

        // 禁止程序修改is_store_s3为1
        if (!empty($changeFiled['is_store_s3'])) {
            $this->is_store_s3 = 0;
        }

        $this->handleProductScene();

        unset($this->promotion_price);
        unset($this->src_price);
        unset($this->promo_info);
        //dd($post['Product']);
        if (!empty($post['Product'])) {
            $this->supply         = array_key_exists('supply', $post['Product']) ? $post['Product']['supply'] : '';
            $this->supply_product = array_key_exists('supply_product', $post['Product']) ? $post['Product']['supply_product'] : '';
        }

        if (empty($this->rating)) {
            $this->rating = 5;
        }

        if (empty($this->created_at)) {
            $this->created_at = Carbon::now();
        }
        if (empty($this->updated_at)) {
            $this->updated_at = Carbon::now();
        }

        if (!empty($this->slug)) {
            $this->slug = strtolower($this->slug);
        }

        if ($this->edit_status == 'chinese_finished' && empty($this->chinese_editor_id)) {
            $this->chinese_editor_id   = BackendAuth::getUser()->id;
            $this->chinese_finished_at = Carbon::now();
        }
        if ($this->edit_status == 'foreign_language_finished' && empty($this->foreign_language_editor_id)) {
            $this->foreign_language_editor_id   = BackendAuth::getUser()->id;
            $this->foreign_language_finished_at = Carbon::now();
        }
        unset($this->foreign_language_finished_at);
        if(isset($this->preferential_status)){
            if(Settings::fetch('enabled_single_preferential')){
                SinglePro::updateProduct($this);
            }
            unset($this->preferential_status);
            unset($this->preferential_price);
            unset($this->prefer_product_id);
        }
        unset($this->discount);
        unset($this->url);
        //(int)post('isFromUpdateCategory') 确保是编辑商品时表单提交修改商品分类，因为商品可以不勾选分类保存
        if ((int)post('isFromUpdateCategory', 0) && empty(post('Product.categories'))) {
            $this->categories = [];
        }
    }

    /**
     * Notes:过滤content中的link，style，script标签
     * User: ma
     * Date: 2021/4/28
     * Time:16:34
     */
    public function filterContent($content)
    {
        $content = preg_replace('/<script[\s\S]*?<\/script>/i', '', $content);
        $content = preg_replace('/<style[\s\S]*?<\/style>/i', '', $content);
        $content = preg_replace('/<link[\s\S]*?>/i', "", $content);
        return $content;
    }
    /**
     * 保存skus后检查改产品是否所有SKU都为无货
     */
    public function syncStatus(){
        if($this->status == 'instock'){
            $id = $this->id;
            $skus = ProductSku::query()->where('product_id',$id)->get();
            $sku_status = false;
            $status = 'stockout';
            foreach($skus as $sku){
                if($sku->sku_status == '1'){
                    $sku_status = true;
                }
            }
            if(!$sku_status){
                \DB::table('cc_products')->where('id',$id)->update(['status' => $status,'sort' => 1000]);
                if (!in_array($status, ProductService::$instockStatus)) {
                    ProductService::deleteProductsFromCache($id);
                }
                
                $this->status = $status;
                $current_time = Carbon::now()->toDateTimeString();
                $backendUser = BackendAuth::getUser();
                $to_save = [
                    'field' => 'status',
                    'old_value' => 'instock',
                    'new_value' => $status,
                    'revisionable_type' => "Jason\Ccshop\Models\Product",
                    'revisionable_id' => $id,
                    'user_id' => $backendUser?$backendUser->id:1,
                    'cast' => '',
                    'created_at' => $current_time,
                    'updated_at' => $current_time,
                    'source_content' => __CLASS__ . '\\' . __FUNCTION__.'---url:'.\Request::url()
                ];
                if (!empty($to_save)) {
                    DB::table('system_revisions')->insert($to_save);
                }
            }
        }
    }

    /**
     * 无货检查，如果是其他组合产品的子产品，则将其组合产品的sku也改成无货。
     */
    public function syncGroupProductStatus(){
        if(in_array($this->status,ProductService::$instockStatus)){
            $skus = ProductSku::query()->where('product_id',$this->id)->where('sku_status','0')->lists('sku');
            if (!$skus){
                return false;
            }
            $groupProductSkus = GroupProductSku::query()->whereIn('item_product_sku', $skus)->lists('group_product_sku');
        }else{
            $groupProductSkus = GroupProductSku::query()->where('item_product_id', $this->id)->lists('group_product_sku');
        }
        if (!$groupProductSkus){
            return false;
        }
        ProductSku::query()->whereIn('sku',$groupProductSkus)->update(['sku_status'=>'0']);
    }

    public function afterSave()
    {
        //手动勾选排序
        $isorts  = post('sort');
        $scores = post('score');
        $productIds = post('checked');
        if(count($isorts) > 0){
            foreach ($isorts as $k => $v) {
                $updateRelateData = [];
                if(!$k){
                    continue;
                }
                if ($v !== "") {
                    $updateRelateData['sort'] = $v;
                }
                if(!empty($scores[$k])){
                    $updateRelateData['score'] = $scores[$k];
                }
                if(empty($updateRelateData)){
                    continue;
                }
                ProductRelate::query()->where('id',$k)->update($updateRelateData);
            }
        }
        if(App::runningInBackend()){
            //创建新标签
            $backendUser = BackendAuth::getUser();
            $backend_uid = $backendUser->id ?? 1;
            $newTags = post('NewTags',[]);
            if(!empty($newTags)){
            $bindTagIds = [];
            if(!empty($newTags['name_cn']) && is_array($newTags['name_cn'])){
                $repeatTag = [];
                foreach($newTags['name_cn'] as $k=>$v){
                    $name = $newTags['name'][$k] ?? $v;

                    if(in_array($name,$repeatTag)){
                        continue;
                    }
                    $repeatTag[] = $name;
                    if(empty($v)){
                        continue;
                    }
                    $tag = Tag::where('name',$name)->where('backend_uid',$backend_uid)->select('id','name','name_cn')->first();
                    if($tag){
                        if(empty($tag->name_cn)){
                            Tag::where('id',$tag->id)->update(['name_cn'=>$name]);
                        }
                        $bindTagIds[] = $tag->id;
                        continue;
                    }
                    $newTagData = ['name'=>$name,'is_public'=>$newTags['is_public'][$k] ?? 0, 'name_cn'=>$newTags['name_cn'][$k] ?? ''];
                    
                    if(str_contains($name,'#')){
                        throw new \ValidationException([
                            'error' => '标签名称包含特殊字符，不能提交'
                        ]);
                    }

                    $tag = Tag::create($newTagData);
                    $bindTagIds[] = $tag->id;
                }
            }
            //绑定标签
            $currentTags = $this->new_tags;
            $currentTagIds =  [];
            if(!empty($currentTags)){
                $currentTagIds = array_column($currentTags->toArray(),'id'); 
            }
            //添加标签
            $addTagIds = array_diff($bindTagIds,$currentTagIds);
                if (!empty($addTagIds)) {
                    $addTagData = [];
                    foreach ($addTagIds as $tag_id) {
                        $addTagData[] = ['tag_id' => $tag_id, 'product_id' => $this->id];
                    }
                    DB::table('cc_products_to_tags')->insert($addTagData);
                    ProductService::updateTagsCount([$this->id]);
                    //添加商品绑定标签记录
                    foreach ($addTagIds as $tag_id) {
                        ProductService::updateTagProductCount($tag_id);
                        SystemRevisionService::saveActionRecord($this->id, 'product_tag', $tag_id, 0,
                            get_class($this));
                    }
                }
            }

        }
        
        //获取变更字段
        $changeFiled = $this->getDirty();

        //商店设置开启product_source_sync_erp，修改产品货源时同步erp
        if (isset($changeFiled['product_source']) && Settings::fetch('product_source_sync_erp', 0)) {
            $productSource = ['product_source' => $changeFiled['product_source'], 'product_id' => $this->id];
            \Queue::push(SyncProductSourceToErp::class, $productSource);
        }

        //根据产品的编辑状态来判断
        if (array_key_exists('edit_status', $changeFiled) and in_array($changeFiled['edit_status'], ['chinese_finished', 'foreign_language_finished'])) {
            //给产品编辑日志中插入产品id和用户id。
            $backendUser = BackendAuth::getUser();
            if ($backendUser) {
                $today    = Carbon::today()->toDateTimeString();
                $tomorrow = Carbon::tomorrow()->toDateTimeString();
                $log      = ProductEditorLog::where('product_id', $this->id)->where('uid', $backendUser->id)->whereBetween('created_at', [$today, $tomorrow])->first();
                //如果该用户今天已经修改过这个产品一次。则不再记录。
                if (empty($log)) {
                    ProductEditorLog::create(['product_id' => $this->id, 'uid' => $backendUser->id, 'status' => $changeFiled['edit_status']]);
                }
            }
        }

        $pointPrices = post('PointPrices', []);
        $rewardPoint = post('RewardPoint', []);
        $usePoint = post('UsePoint', []);
        $Rebates     = post('Rebate', []);
        $tags        = post('tags');
        $components  = post('components', '');
        $skus        = post('skus');
        $productSizes = post('productSizes',[]);
        $productSizeGroup = post('productSizeGroup',[]);
        $ReviewsLabels = post('ReviewsLabels', []);

        //save the point prices data
        if ($this->is_pbp && !empty($pointPrices['point_price']) && intval($pointPrices['point_price']) > 0) {
            $data = ProductPointPrice::where(['product_id' => $this->id])->first();
            if ($data) {
                $data->point_price = $pointPrices['point_price'];
                $data->save();
            } else {
                ProductPointPrice::create([
                    'product_id'  => $this->id,
                    'point_price' => $pointPrices['point_price'],
                ]);
            }
        }
        //save the reward point data
        if ($rewardPoint && is_array($rewardPoint)) {
            foreach ($rewardPoint as $key => $point) {
                if (is_numeric($point['amount'])) {
                    $where = [
                        'bind_id'    => $this->id,
                        'bind_type'  => 'Jason\Ccshop\Models\Product',
                        'user_group' => $point['user_group'],
                    ];
                    $data = RewardPoint::where($where)->first();
                    if ($data) {
                        $data->amount      = $point['amount'];
                        $data->amount_type = $point['amount_type'];
                        if ($point['amount'] == 0) {
                            $data->delete();
                        } else {
                            $data->save();
                        }
                    } else {
                        $data = array_merge($where, $point);
                        RewardPoint::create($data);
                    }
                }
            }
        }

        //save the use point data
        if ($usePoint && is_array($usePoint)) {
            foreach ($usePoint as $key => $point) {
                UsePoint::saveUsePoint($this->id,'Jason\Ccshop\Models\Product',$point);
            }
        }

        //save the rebate data
        if ($Rebates && is_array($Rebates)) {
            foreach ($Rebates as $key => $rebate) {
                if (empty($rebate['amount'])) {
                    continue;
                }
                $where = ['bind_id' => $this->id, 'bind_type' => get_class()];
                $data  = Rebate::where($where)->first();
                if ($data) {
                    $data->amount      = $rebate['amount'];
                    $data->amount_type = $rebate['amount_type'];
                    $data->save();
                } else {
                    $data = array_merge($where, $rebate);
                    Rebate::create($data);
                }
            }
        }

        //save the product tags
        if (isset($tags)) {
            $where = ['type'=>Tag::TYPE_CUSTOM,'taggable_id' => $this->id, 'taggable_type' => get_class()];
            TagGable::join('cc_tags','cc_tags.id','=','cc_tag_gables.tag_id')->where($where)->delete();
            if ($tags) {
                $tagsArr   = explode(',,,', $tags);
                $tagGables = [];
                foreach ($tagsArr as $tag) {
                    if (is_numeric($tag)) {
                        $tag = Tag::find($tag);
                        if (empty($tag)) {
                            $tag = Tag::firstOrCreate(['name' => $tag, 'product_ids' => $this->id]);
                        } else {
                            Tag::where('id',$tag->id)->update(['product_ids'=>trim(implode(',', array_unique(array_merge(explode(',', $tag->product_ids), [$this->id]))), ',')]);
                        }
                    } else {
                        $tag = Tag::firstOrCreate(['name' => $tag, 'product_ids' => $this->id]);
                    }
                    $tagGables[] = [
                        'tag_id'        => $tag->id,
                        'taggable_id'   => $this->id,
                        'taggable_type' => get_class(),
                    ];
                }
                TagGable::insert($tagGables);
            }
        }
        // save components
        if ($components) {
            $where = ['bind_id' => $this->id, 'bind_type' => get_class(), 'name' => 'components'];
            CustomField::where($where)->delete();
            $componentArr = explode(',,,', $components);
            collect($componentArr)->map(function ($component) {
                $data = ['bind_id' => $this->id, 'bind_type' => get_class(), 'name' => 'components', 'value' => $component];
                CustomField::create($data);
            });
        }

        // 保存skus组合的价格、sku状态、库存数
        if (count($skus)) {
            foreach($skus as $skuId => $sku){
                $skuObj = ProductSku::where('id', $skuId)->first();
                if(!empty($skuObj)){
                    isset($sku['sku_price']) && $skuObj->sku_price = $sku['sku_price'];
                    isset($sku['sku_status']) && $skuObj->sku_status = $sku['sku_status'];
                    isset($sku['sku_stock']) && $skuObj->sku_stock = $sku['sku_stock'];
                    isset($sku['sku_sale_price']) && $skuObj->sku_sale_price = $sku['sku_sale_price'];
                    $skuObj->is_enabled_sku_sale_price = $sku['is_enabled_sku_sale_price'] ?? 0;
                    if( isset($sku['sku_price']) ||  isset($sku['sku_status']) || isset($sku['sku_stock']) || isset($sku['sku_sale_price']) || isset($sku['is_enabled_sku_sale_price'])) {
                        $skuObj->save();
                    }
                }
            }
        }

        //保存评论标签  删除评论标签
        $ProductReviewsLabelIds = ProductReviewsLabel::where('product_id',$this->id)->lists('id');
        if($ReviewsLabels){
            foreach($ReviewsLabels as $label){

                if(!empty($label['id'])){
                    ProductReviewsLabel::where('id',$label['id'])->update(['num' => $label['num'], 'sort' => $label['sort']]);
                }else{
                    $prl_data = [ 'product_id' => $this->id, 'reviews_label_id'=> $label['reviews_label_id'], 'num' => $label['num']??0, 'sort' => $label['sort']??0];
                    ProductReviewsLabel::create($prl_data);
                }
            }
        }
        //删除评论标签
        $disff_ids = array_diff($ProductReviewsLabelIds, array_column($ReviewsLabels,'id')??[]);
        if($disff_ids){
            ProductReviewsLabel::where('product_id',$this->id)->whereIn('id',$disff_ids)->delete();
        }

        $this->syncStatus();
        $this->syncGroupProductStatus();

        //处理后台编辑的尺码表
       $this->handleProductSize($productSizes);

        if ( $productSizeGroup)
        {
            $ProductSizeObj = ProductSize::where('product_id',$this -> id) -> first();
            $resGroup = [];

            foreach ($productSizeGroup as $key => $size) {
                $size_array = [];
                if(!empty($size['size_value'])){
                    foreach($size['size_value'] as $k => $value ){
                        $size['size_value'][$k]['values'] = array_values($value['values']);
                        $size_array[] = array_merge($value,$size['size_value'][$k]);
                    }
                    $resGroup[$key]['size'] = $size['size'];
                    $resGroup[$key]['size_value'] = $size_array;
                }else{
                    $enabledSizeDelete = Settings::fetch('enabled_delete_sizes', false);//是否允许删除清空尺码
                    if(!$enabledSizeDelete){//允许删除尺码
                       return;
                    }
                }

            }
            $ProductSizeObj -> size_group = json_encode($resGroup,JSON_UNESCAPED_UNICODE);

            $ProductSizeObj -> save();
        }


        // 存储排序值,使用北京时间
        $date      = Carbon::now('Asia/Shanghai')->toDateString();
        $todaySort = ProductIntArchive::where(['product_id' => $this->id, 'type' => 'S', 'date' => $date])->first();
        if ($todaySort && $todaySort->value != $this->sort) {
            $todaySort->value = $this->sort ? $this->sort : 999;
            $todaySort->save();
        }
        if (empty($todaySort)) {
            $sort = ProductIntArchive::where(['product_id' => $this->id, 'type' => 'S'])->where('date', '<', $date)->orderBy('date', 'desc')->pluck('value');
            if (($sort && $sort != $this->sort) || empty($sort)) {
                ProductIntArchive::create([
                    'product_id' => $this->id,
                    'type'       => 'S',
                    'value'      => $this->sort ? $this->sort : 0,
                    'date'       => $date,
                ]);
            }
        }
        self::lagCategoryProductSort($this->id);
        
        // 更新feature
        if (post('from_backend_submit_button', '') == 1) {
            $sceneId = post('scene', 0);
            $features = post('fv', []);
            $parten   = '/\[(.*?)\]/';
            ProductToFeature::where(['scene_id' => $sceneId,'product_id' => $this->id])->delete();
            if (count($features)) {
                foreach ($features as $feature) {
                    preg_match_all($parten, $feature, $match);
                    $featureId      = $match[1][0];
                    $featureValueId = $match[1][1];
                    ProductToFeature::create([
                        'scene_id' => $sceneId,
                        'product_id' => $this->id,
                        'feature_id' => $featureId,
                        'feature_value_id' => $featureValueId
                    ]);
                }
            }
        }


        //给统计信息添加产品状态（石小梅）
        \Jason\Ccshop\Models\ProductStatistic::saveStatInfo(['product_id' => $this->id],
            ['status' => $this->status, 'price'=>$this->price, 'sort'=>$this->sort, 'pid'=>$this->pid ? $this->pid : intval($this->spu)]);


        if (array_key_exists('status', $changeFiled) and $changeFiled['status'] == 'instock') {
            //当前商品编辑在库时发送该商品到智能商城测试站
            if(Settings::fetch("smart_mall_sync_send_enable", false)){
                \Jason\Ccshop\Classes\Smartmall\ProductsSort::pushProductSmartmall($this->id);
            }
            //选品上新完成
            \Jason\Ccshop\Controllers\SelectionProducts::instockSelectionProduct($this->spu);

        }

        if (array_key_exists('status', $changeFiled) && $changeFiled['status'] == 'instock') {
            ProductService::productStatusChangeJob($this->spu, $this->id);
        }
        //下架商品处理组合购成员状态
        if (array_key_exists('status', $changeFiled) && $changeFiled['status'] == 'shelve') {
            \Cache::tags('group-purchase')->flush(); //清理组合购缓存
        }
        //没有库存状态商品发送钉钉通知
        if (array_key_exists('status', $changeFiled) && $changeFiled['status'] == 'stockout') {
            ProductService::sendStockoutDingMsg($this->id,$this->original['status'],$backendUser??'');
        }

        if (array_key_exists('status', $changeFiled)) {
            // 更改产品状态 清理 getNewlyInstockIds 缓存
            Cache::tags('newly-instock-product-ids')->flush();
            // 更新产品状态缓存
            ProductService::rebuildProductStatusCache([$this->id => $this->status]);
            if (!in_array($this->status, ProductService::$instockStatus)) {
               // 产品状态非在库和到货提醒删除缓存数据
                ProductService::deleteProductsFromCache($this->id);
                info(sprintf('产品 %d 状态变更为: %s, 删除持久化缓存', $this->id, $this->status));
            }
        }

        // 清理产品路由缓存
        ProductService::clearProductRouteCache([$this->id]);

        ProductService::putReCalculatePromotionPriceProducts([$this->id]);

        //此处更新索引转移至，产品控制器中的formAfterSave()

        // Jobs::putRedumpPid($this->id);

        //更新产品收藏数缓存（防止穿透 直接更新）
        WishlistService::updateCacheToWishTotal($this->id,$this->CustomFields['wish_total'] ?? 0);

        // 当状态或分类有改动时清除优先勾选位积分设置(algorithms类型)缓存
        $categories = post('Product.categories', []);
        $initCategoryIds = post('initCategoryIds', '');
        $categoryStrIds = implode(',', $categories);
        if (array_key_exists('status', $changeFiled) || $categoryStrIds != $initCategoryIds) {
            ProductService::clearFixSectionAndPointCache($this->id);
        }
        //清除关联产品缓存                                    
        Cache::tags('relation')->forget($this->id);  
    }

    /**
     * 处理产品场景数据
     */
    private function handleProductScene()
    {
        /* Scene start */
        $sceneId = post('scene');
        $sceneProductFormData = post('SceneProduct');
        if (!empty($sceneId) && !empty($sceneProductFormData)) {
            $validator = \Validator::make(
                $sceneProductFormData,
                [
                    'name'       => 'required|between:1,255',
                    'slug'       => 'required|alpha_dash|between:1,255',
                    'price'      => 'required|numeric|digits_between:1,14',
                    'list_price' => 'required|numeric|digits_between:1,14',
                    'sort'       => 'required|numeric',
                ]
            );

            if ($validator->fails()) {
                throw new \ValidationException($validator);
            }

            $sceneProduct = ProductScene::where('scene_id', $sceneId)->where('product_id', $this->id)->first();
            if ($sceneProduct) {
                $updateData = [];
                foreach ($sceneProductFormData as $key => $value) {
                    if ($sceneProduct->{$key} != $value) {
                        $updateData[$key] = $value;
                    }
                }
                $sceneProduct->update($updateData);
            } else {
                ProductScene::create([
                    'scene_id' => $sceneId,
                    'product_id' => $this->id,
                    'name' => data_get($sceneProductFormData, 'name'),
                    'slug' => data_get($sceneProductFormData, 'slug'),
                    'price' => data_get($sceneProductFormData, 'price'),
                    'list_price' => data_get($sceneProductFormData, 'list_price'),
                    'sort' => data_get($sceneProductFormData, 'sort'),
                ]);
            }
        }
    }

    //处理尺码
    public function handleProductSize($productSizes){
        $ProductSizeObj = ProductSize::where('product_id',$this->id) -> first();
        // 保存尺码表
        if ($productSizes)
        {
            $sizes = json_decode($ProductSizeObj -> sizes ?? '[]',true);
            $res = [];
            $newArray = [];
            foreach ($productSizes as $key => $value) {
                //重置索引
                $value['values'] = array_values($value['values']);
                $newArray[$value['id']] = $value;
                unset($productSizes[$key]);
            }
            foreach($sizes as $key => $value)
            {
                if (!array_key_exists($value['id'], $newArray)) {
                    unset($sizes[$key]);
                    continue;
                }
                $res[] = array_merge($value,$newArray[$value['id']]);
                unset($newArray[$value['id']]);
            }

            $res = array_merge($res, $newArray);
            //重置索引
            $res = array_values($res);
            if(!$ProductSizeObj){//没有尺码则创建
                ProductSize::Create(
                    [
                        'product_id'=>$this->id,
                        'sizes'=>json_encode($res,JSON_UNESCAPED_UNICODE),
                        'size_image'=>'',
                        'size_group'=>''
                    ]);
            }else{
                $ProductSizeObj -> sizes = json_encode($res,JSON_UNESCAPED_UNICODE);
                $ProductSizeObj -> save();
            }
        }else{
            if($ProductSizeObj){
                $enabledSizeDelete = Settings::fetch('enabled_delete_sizes', false);//是否允许删除清空尺码
                if($enabledSizeDelete){//允许删除尺码
                    $ProductSizeObj->sizes=json_encode([]);
                    $ProductSizeObj->save();
                }
            }
        }
    }
    //修改商品的分类内排序
    public static function lagCategoryProductSort($pid){
        $productCategories = ProductCategory::where('product_id',$pid)->get()->toArray();

        $product = Product::find($pid);
        $pcids = array_column($productCategories,'category_id');
        $maxSorts = ProductCategory::whereIn('category_id', $pcids)->groupBy('category_id')->select(Db::raw('max(`sort`) as maxSort'), 'category_id')->lists('maxSort', 'category_id');
        foreach($productCategories as  $productCategory){
            //将商品分类的sort值改为当前分类下的最后一个
            if($productCategory['sort'] == 0 || !in_array($product->status,['stocktension','instock'])){
                ProductCategory::where('product_id',$product->id)->where('category_id',$productCategory['category_id'])->update(['sort'=>($maxSorts[$productCategory['category_id']] ?? 0) + 1]);
            }
        }

        $originCids = array_column($productCategories,'category_id');
        \Jason\Ccshop\Classes\SubNameProductSort::syncCategoryProducts($originCids,$pid);
        return true;
    }


    public function beforeCreate()
    {
        // 记录发布人
        $backendUser = BackendAuth::getUser();
        if ($backendUser) {
            $this->admin_id = $backendUser->id;
        }
    }


    public function afterCreate()
    {
        //根据分类查找绑定的特征组及特征，并自动添加
        if (count($this->categories) > 0) {
            foreach ($this->categories as $category) {
                if (count($category->feature_groups) == 0) {
                    continue;
                }
                foreach ($category->feature_groups as $group) {
                    if (count($group->features) == 0) {
                        continue;
                    }
                    foreach ($group->features as $feature) {
                        $data = [
                            'product_id' => $this->id, 'feature_id' => $feature->id, 'feature_value_id' => 0,
                        ];
                        $exists = ProductToFeature::where($data)->count();
                        if ($exists == 0) {
                            ProductToFeature::create($data);
                        }
                    }
                }
            }
        }
        //根据勾选的产品公共选项，自动创建成私有选项
        if (count($this->options) > 0) {
            foreach ($this->options as $option) {
                $data              = $option->toArray();
                $data['parent_id'] = $data['id'];
                unset($data['id']);
                $data['product_id'] = $this->id;
                $newOption          = Option::create($data);
                if ($newOption && count($option->option_values) > 0) {
                    foreach ($option->option_values as $value) {
                        $dataValue = $value->toArray();
                        unset($dataValue['id']);
                        $dataValue['option_id'] = $newOption->id;
                        OptionValue::create($dataValue);
                    }
                }
            }
        }

        if (empty($this->code)) {
            $this->code = $this->id;
            $this->save();
        }

        if (Settings::fetch("check_product_unique", false)) {
            session(['has_check_unique' => false]);
        }

        //更新可用产品数量
        if (Cache::has('enabled_product_total')) {
            Cache::increment('enabled_product_total');
        } else {
            Cache::forever('enabled_product_total', Product::InStock()->count('id'));
        }
    }

    public function afterDelete()
    {
        (new Products)->onDeleteProductIds($this->id);
        if (Cache::has('enabled_product_total')) {
            Cache::decrement('enabled_product_total');
        } else {
            Cache::forever('enabled_product_total', Product::InStock()->count('id'));
        }

        $es          = Searches::esInstance();
        $langs       = Locale::listEnabled();
        $esIndexName = Searches::getSearchEngineIndexName();
        foreach ($langs as $code => $lang) {
            $indexFullName = $esIndexName . '-' . $code;
            try {
                $es->delete(['index' => $indexFullName, 'type' => 'products', 'id' => $this->id]);
            } catch (\Exception $e) {}
        }
        $this->insertDeleteRecord();

        ProductService::deleteProductsFromCache($this->id);
        ProductService::deleteProductStatusFromCache($this->id);
    }

    /** 加删除记录
     * Notes:
     * User: ma
     * Date: 2021/6/15
     * Time:15:08
     * @param $data
     */
    public function insertDeleteRecord()
    {
        $user = BackendAuth::getUser();

        $model = new DeleteRecord();
        $model->uid = $user->id;
        $model->model = get_class($this);
        $model->primary = $this->id;
        $model->original_data = json_encode($this);
        $model->save();
        $dateday = date('Y-m-d H:i:s', time());
        $eventLog = '用户：'.$user->id."-".$user->email.'在'.$dateday.'删除了ID为'.$this->id.'的商品';
        info("删除商品记录:".$eventLog);
    }

    public function afterFetch()
    {
        $labels = [];
        $sellingPrice = $this->sellingPrice($labels);
        $this->promo_info = $labels;
        $src_price             = (string)$sellingPrice['src_price'];
        $promotion_price       = (string)$sellingPrice['price'];
        if(empty(Settings::fetch('is_down_usd_price_ceil','0'))){  //美元站是否开启取整
            $src_price       = ceil($src_price);
            $promotion_price = ceil($promotion_price);
        }
        $this->src_price       = $src_price;
        $this->promotion_price = $promotion_price;

        if (! App::runningInBackend()) {
            $this->list_price       = ceil((string)$sellingPrice['list_price']);
            if ($this->promotion_price < $this->list_price) {
                $this->price = $this->promotion_price;
            }
            $this->discount = $this->getDiscount();
        }

        $this->url = $this->getUrl();
        if (Settings::fetch('enabled_single_preferential')) {
            $singlePro = SinglePro::getProduct($this->id);
            if (! empty($singlePro)) {
                $this->preferential_status = $singlePro->preferential_status;
                $this->preferential_price = $singlePro->preferential_price;
                $this->prefer_product_id = $singlePro->prefer_product_id;
            }
        }
    }

    /**
     *  获取主要的货源链接并处理
     *
     * @return string
     */
    public function getMailSourceUrl()
    {
        try {
            $fisrtUrl = $this->product_source['url']['1'];
            $url = strtok($fisrtUrl,'?');
            $urlArr = parse_url($fisrtUrl);
            if (key_exists('query', $urlArr)) {
                $queryArr = explode('&', $urlArr['query']);
                foreach ($queryArr as $q) {
                    if (str_contains($q, 'goodscode') OR str_contains($q, 'id')) {
                        $url = $url.'?'.$q;
                        break;
                    }
                }
            }

            return $url;
        } catch (\Exception $exception) {
            return '';
        }
    }

    /**
     * es 获取产品付款(未付款,已付款)数
     * @param $is_status  true 付款数  false  (未付款,已付款)
     * @return mixed
     */

    public function esSaledTotal(bool $is_status)
    {
        $must_list  = $must_nested_list = [];
        $must_nested_list[] = ['term'=>['products.product_id' => $this->id]];
        if ($is_status === true) {
            $saledIds = OrderStatus::saledStatusIds();
            $must_list[] = ['terms' => ['status_id' => $saledIds]];
        }
        $must_list[] = [
            'nested'=>[
                'path' => 'products',
                'query' => [
                    'bool' => [
                        'must' => [
                            $must_nested_list
                        ]
                    ]
                ]
            ]
        ];
        $es_query = [
            'query' => [
                'bool' => [
                    'must' => $must_list
                ]
            ],
            'size'=>0,
            'aggs' => [
                'data' => [
                    'nested' => ['path' => 'products'],
                    'aggs' => [
                        'items'=>[
                            'filter' => ['term' => ['products.product_id' => $this->id]],
                            'aggs'=>[
                                'sum_qty' => ['sum' => ['field' => 'products.qty']],
                            ]
                        ],
                    ]
                ]
            ]
        ];
        $options = ['suffix' => 'order', 'type' => 'orders'];
        $sum_qty  = Searches::searchDataList($es_query, $options);
        $sum_qty  = $sum_qty['aggregations']['data']['items']['sum_qty']['value']??0;
        return $sum_qty;
    }

    public function getSaledTotalAttribute($value)
    {
        $cacheKey = 'product-saled-total-' . $this->id;
        return Cache::remember($cacheKey, 1440, function () {
            $sum = OrderProduct::query()->where('product_id', $this->id)->whereHas('order', function ($query) {
                $query->Saled();
            })->sum('qty');
            return !empty($sum) ? $sum : 0;
        });
    }

    public function getDiscount($listPrice = null, $price = null,$promotion_price = null)
    {
        if (!$listPrice){
            $listPrice = $this->list_price;
        }
        if (!$price){
            $price = $this->price;
        }
        if(!$promotion_price){
            $promotion_price = $this->promotion_price;
        }
        if ($listPrice != 0 && $listPrice > $price) {
            $discount = bcmul(bcdiv(bcsub($listPrice,$price),$listPrice,3),100);
            $discount = intval($discount);
            $discount = $discount . '%';
        } else {
            $discount = null;
        }

        //促销折扣处理
        if ($promotion_price && $listPrice > $promotion_price && $listPrice != 0) {
            $discount = bcmul(bcdiv(bcsub($listPrice,$promotion_price),$listPrice,3),100);
            $discount = intval($discount);
            $discount = $discount . '%';
        }

        return $discount;
    }

    public function getUrl()
    {
        $id        = $this->id;
        $slug      = $this->slug;
        $urlFormat = Settings::fetch('product_url_format', '/{slug}-p-{id}.html');
        $url       = str_replace('{id}', $id, $urlFormat);
        $url       = str_replace('{slug}', $slug, $url);

        if (\Jason\Ccshop\Models\Settings::fetch('enabled_product_url_for_categories', '0') == 1) {
            if (count($this->categories) < 3) {
                $slug_cate1 = isset($this->categories[0]['slug']) ? $this->categories[0]['slug'] : 'best';
                $slug_cate2 = isset($this->categories[1]['slug']) ? $this->categories[1]['slug'] : 'goods';
            } else {
                $slug_cate1 = isset($this->categories[1]['slug']) ? $this->categories[1]['slug'] : 'best';
                $slug_cate2 = isset($this->categories[2]['slug']) ? $this->categories[2]['slug'] : 'goods';
            }

            $url = $slug_cate1 . '-' . $slug_cate2 . '-p-' . $id . '.html';
        }
        $url = str_replace('根分类', 'goods', $url);

        return $url;
    }

    public function getFeatureGroups($value)
    {
        $product   = $this;
        $cacheKey  = 'product-feature-groups-' . $this->id;
        $cacheTags = ['products', 'product-feature-groups', 'product-feature-groups-' . $this->id];
        return Cache::tags($cacheTags)->remember($cacheKey, 1440, function () use ($product) {
            $query = ProductToFeature::where("product_id", $product->id);
            ShopBase::withFactory($query, ['feature', 'feature_value'], ['products', 'product-features']);
            $features = $query->cacheTags(['products', 'product-to-features'])
                ->remember(1440)
                ->get();

            if (count($features) == 0) {
                return [];
            }

            $groups = [];
            foreach ($features as $feature) {
                $group = FeatureGroups::getGroup($feature->feature->group_id);
                if ($feature->feature && count($group)) {
                    if (!isset($groups[$group->id])) {
                        $groups[$group->id] = $group->toArray();
                    }
                    $groups[$group->id]['features'][] = $feature;
                }
            }
            return $groups;
        });
    }

    public function getCustomFieldsAttribute($value)
    {
        return CustomField::getAllFieldValues($this);
    }

    public function getCategoriesIdsAttribute($value)
    {
        return ProductCategory::query()->where("product_id", $this->id)->lists('category_id');
    }

    /*
     * 购物车，结算页面产品积分计算
     * 返回该产品对应用户的积分
     */
    public function getPointAttribute()
    {
        $user = (new Account)->user();
        if ($user) {
            $userGroups = $user->groups->lists('id');
            return Helper::getRewardPoint($this, $userGroups, $this->price);
        } else {
            return Helper::getRewardPoint($this, $userGroups = '', $this->price);
        }
    }
    /*
     * 用于ES索引生成产品积分数组
     * 返回数组
     */
    public function getPointsAttribute()
    {
        return Helper::getRewardPoints($this);
    }

    public static function enabledTotal()
    {
        return Cache::remember('enabled_product_total', 1440, function () {
            return Product::query()->InStock()->count('id');
        });
    }

    public function scopeInStock($query)
    {
        return $query->whereIn('status', self::SIMILAR_IN_STOCK_STATUS);
    }
    public function scopeInStockNew($query)
    {
        $ProductSectionSelecteds = ProductSectionSelected::query()->where(['type' => 'new'])->get();
        $productsArray           = [];
        foreach ($ProductSectionSelecteds as $key => $ProductSectionSelected) {
            $productsArray[$key] = $ProductSectionSelected->product_id;
        }
        return $query->whereIn('status', ['instock', 'stockout'])->whereIn('cc_products.id', $productsArray);
    }

    public function scopeInStockSelect($query)
    {
        $ProductSectionSelecteds = ProductSectionSelected::where([
            'type' => '',
        ])->get();
        $productsArray = [];
        foreach ($ProductSectionSelecteds as $key => $ProductSectionSelected) {
            $productsArray[$key] = $ProductSectionSelected->product_id;
        }
        return $query->whereIn('status', self::SIMILAR_IN_STOCK_STATUS)->whereIn('cc_products.id', $productsArray);
    }

    public function scopeInCategory($query, $categories)
    {
        return $query->whereHas('categories', function ($q) use ($categories) {
            $q->whereIn('id', $categories);
        });
    }

    public function scopeInChain($query, $chain)
    {
        if (count($chain) == 1) {
            $pid = DB::table('cc_product_chain_related')->lists('product_id');
            if (in_array(1, $chain)) {
                //关联
                return $query->whereIn('id', $pid);
            } else if (in_array(2, $chain)) {
                //未关联
                return $query->whereNotIn('id', $pid);
            }
        }
        return '';
    }

    public function scopeInUsers($query, $uids)
    {
        return $query->whereIn('admin_id', $uids);
    }

    public function scopeInEditor($query, $uids)
    {
        return $query->whereIn('update_id', $uids);
    }

    public function scopeInChineseEditor($query, $uids)
    {
        return $query->whereIn('chinese_editor_id', $uids);
    }

    public function scopeInForeignEditor($query, $uids)
    {
        return $query->whereIn('foreign_language_editor_id', $uids);
    }

    //在库理由筛选
    public function scopeInInstockReasonEditor($query, $ids)
    {
        $key = array_search('all', $ids);
        if($key !== false){
            $ids[$key] = 0;
        }
        return $query->join('cc_instock_reason','cc_instock_reason.product_id','=','cc_products.id')->groupBy('cc_instock_reason.product_id')->whereIn('cc_instock_reason.reason',$ids);
    }

    //产品标签筛选
    public function scopeProductTag($query, $ids)
    {
        return $query->join('cc_tag_gables','cc_tag_gables.taggable_id','=','cc_products.id')->groupBy('cc_tag_gables.taggable_id')->whereIn('cc_tag_gables.tag_id',$ids)->where('cc_tag_gables.taggable_type','Jason\Ccshop\Models\Product');
    }

    public function scopeNoThumb($query)
    {
        return $query->whereHas('thumb', '=', 0)->whereHas('feature_image', '=', 0)->get();
    }

    public function scopeInLabel($query, $labelId)
    {
        return $query->whereHas('product_label', function ($q) use ($labelId) {
            $q->whereIn('label_id', $labelId);
        });
    }

    public function getBrandsOptions()
    {
        $brandOptions    = ProductBrand::lists('name', 'id');
        $brandOptions[0] = e(trans('jason.ccshop::lang.product.no_brand'));
        return $brandOptions;
    }

    public function scopeInPlaceOrderRange($query, $begin, $end)
    {
        $begin = $begin->toDateTimeString();
        $end = $end->toDateTimeString();

        $paid = OrderStatus::findByCode('paid');

        return $query->whereHas('orders', function ($q) use ($begin, $end, $paid) {
            $q->whereBetween('created_at', [$begin, $end])
                ->whereHas('status_history', function ($q) use ($paid) {
                    $q->where('status_id', $paid->id);
                });
        });
    }

    /**
     * 根据下单时间获取产品销量
     * 为了数据实时性准确，从数据库查询
     * 请勿用于前台调用！！
     * @param $begin
     * @param $end
     * @return int|mixed
     */
    public function getSalesVolumeByPlaceOrderRange($begin, $end)
    {
        $begin = Carbon::parse($begin)->toDateTimeString();
        $end = Carbon::parse($end)->toDateTimeString();

        $sum = OrderProduct::query()->where('product_id', $this->id)
            ->whereHas('order', function ($query) use ($begin, $end) {
                $query->whereBetween('created_at', [$begin, $end])
                    ->whereHas('status_history', function ($q) {
                        $q->whereHas('status', function ($q) {
                            $q->where('code', 'paid');
                        });
                    });
            })->sum('qty');

        return $sum ?: 0;
    }

    public function getSalesVolumeByPlaceOrderRangeFromEs($begin, $end)
    {
        return OrderService::getSalesVolumeByPlaceOrderRangeFromEs($this->id,$begin, $end);
    }

    /**
     * 获取最近一周销量
     * 为了数据实时性准确，从数据库查询
     * 请勿用于前台调用！！
     * @return int|mixed
     */
    public function getWeekSales()
    {
        $begin = Carbon::now()->subWeek()->toDateTimeString();
        $end = Carbon::now()->toDateTimeString();

        return $this->getSalesVolumeByPlaceOrderRange($begin, $end);
    }

    /**
     * 获取最近一周销量
     * @return int|mixed
     */
    public function getWeekSalesFromEs()
    {
        $begin = Carbon::now()->subWeek()->toDateTimeString();
        $end = Carbon::now()->toDateTimeString();

        return OrderService::getSalesVolumeByPlaceOrderRangeFromEs($this->id, $begin, $end);
    }

    /**
     * Delete Specific Product Data
     * @param Integer $productSort
     * @param String $categoryIds
     * @param Integer $stepCurrent
     * @param Integer $handleNumPerRequest
     * @param Array $relations
     * @return Integer $successTotal
     */
    public static function removeSpecificProduct($productSort, $categoryIds, $stepCurrent = 1, $handleNumPerRequest = 1000, $relations = array())
    {
        ini_set('memory_limit', '128M');
        set_time_limit(0);
        $successTotal    = 0;
        $sortThreshold   = $productSort;
        $deleteThreshold = min($handleNumPerRequest, 250);

        $total          = DB::select("select count(id) as total from cc_products where sort > $sortThreshold ");
        $cycleCount     = $total[0]->total < $handleNumPerRequest ? ceil($total[0]->total / $deleteThreshold) : ceil($handleNumPerRequest / $deleteThreshold);
        $categoryNewIds = '';

        if ($categoryIds && $stepCurrent == 1) {
            $categoryIdsArr = explode(',', $categoryIds);
            $Categories     = Category::whereIn('id', $categoryIdsArr)->get();

            if (!empty($Categories)) {
                foreach ($Categories as $category) {
                    $child = $category->getChildrens();
                    foreach ($child as $subCatetory) {
                        $categoryNewIds .= ',' . $subCatetory['id'];
                    }
                }
                $categoryNewIds = substr($categoryNewIds, 1);
                if ($categoryNewIds) {
                    DB::delete("delete from cc_categories where id in(" . $categoryNewIds . ")");
                    if ($sortThreshold > 1) {
                        DB::delete("delete from cc_product_categories where category_id in(" . $categoryNewIds . ")");
                    }
                }
            }
        }

        if ($sortThreshold > 1) {
            for ($i = 0; $i < $cycleCount; $i++) {
                try {
                    $products  = DB::select("select id from cc_products where sort > $sortThreshold limit $deleteThreshold");
                    $deleteIds = [];

                    if (count($products) > 0) {
                        foreach ($products as $product) {
                            $deleteIds[] = $product->id;
                        }

                        $deleteQuery = implode(',', $deleteIds);

                        DB::delete("delete from system_files where  attachment_id in (" . $deleteQuery . ") limit $deleteThreshold");

                        if (!empty($relations)) {
                            foreach ($relations as $table) {
                                DB::delete("delete from $table where product_id in (" . $deleteQuery . ") limit $deleteThreshold");
                            }
                        }

                        DB::delete("delete from cc_products where  id in (" . $deleteQuery . ") limit $deleteThreshold");
                        ++$successTotal;

                    }

                } catch (Exception $ex) {

                }

            }
        }

        return $successTotal;
    }

    public function getRevisionableUser()
    {
        if (!empty(BackendAuth::getUser())) {
            return BackendAuth::getUser()->id;
        } else {
            return !empty(self::$backendUserId) ? self::$backendUserId : 1;
        }
    }

    /**
     * 获取有效的商品通过id集合
     *
     * @author chenfeng (wangchenfeng@xdqcjy.com)
     * @date 2018-03-29
     * @param array $ids 商品ID集
     * @param array $field 查询指定内容
     * @return array
     */
    public static function getEffectiveProductByIds(array $ids, $field = ['id'])
    {
        return self::query()->select($field)->whereIn('id', $ids)->get()->toArray();
    }

    /**
     * 获取产品所有选项的笛卡尔积矩阵和sku
     *
     * @return array
     */
    public function optionsCartesianSkus()
    {
        $allOptionsCartesian = $this->optionsCartesian();
        $plSkus = ProductSku::where('product_id', $this->id)->get()->toArray();
        foreach ($allOptionsCartesian as &$sku) {
            // 搜索获取对应数组序号
            $sku_no = array_search($sku, $allOptionsCartesian);
            // 不足3位的SKU后缀进行前导零补全
            $sku_no = str_pad($sku_no + 1, 3, 0, STR_PAD_LEFT);
            $sku['pl_sku'] = $this->getPlSkus($plSkus, $sku);
            $sku['sku'] = $this->id . $sku_no;
        }
        return $allOptionsCartesian;
    }

    /**
     * 获取产品库的sku
     *
     * @return array
     */
    public function getPlSkus($plSkus, $currentOption)
    {
        $optionLength = count($currentOption);
        $plSku = '';
        if (count($plSkus)) {
            foreach ($plSkus as $sku) {
                $flag = 0;
                $optionValues = json_decode($sku['option_values'], true);
                foreach ($optionValues as $optionId => $value) {
                    if (isset($currentOption[$optionId]) && !empty($value) && isset(array_keys($value)[0]) && $currentOption[$optionId] == array_keys($value)[0]) {
                        $flag++;
                    }
                }
                if ($flag == $optionLength) {
                    $plSku = $sku['sku'];
                    break;
                }
            }
        }
        return $plSku;
    }

    public function beforeDelete()
    {
        OperateLog::setDelProduct($this);
    }

    public function getFeatureImage($width = 500, $height = 500)
    {
        $images = $this->filterRelationProductData('feature_image');
        if($images->isEmpty()){
            return [];
        }

        $lists = [];
        foreach ($images as $k=>$val){
            $lists[$k] = $val->getThumb($width, $height);
        }

        return $lists;
    }

    public function getSpecialPromotionPriceAttribute($value)
    {
        $promotion = new Promotions();
        $promotionIds = $promotion->getSpecialPromotionId();
        if (in_array($this->id, $promotionIds['new']['data'])){
            $promotionDiscount = $promotionIds['new']['discount_mul'];
        }
        if (in_array($this->id, $promotionIds['hot']['data'])){
            $promotionDiscount = $promotionIds['hot']['discount_mul'];
        }
        $price =  !isset($promotionDiscount)? $this->price : $this->price * $promotionDiscount;
        return ceil($price);
    }

    public function getSpecialPromotionDiscountAttribute($value)
    {
        $promotion = new Promotions();
        $promotionIds = $promotion->getSpecialPromotionId();
        if (in_array($this->id, $promotionIds['new']['data'])){
            $discount = $promotionIds['new']['discount'];
        }
        if (in_array($this->id, $promotionIds['hot']['data'])){
            $discount = $promotionIds['hot']['discount'];
        }
        return $discount ?? $this->discount;
    }

    /**
     * 获取商品最近一次售出时间
     * @return string|null
     */
    public function getSaledLatelyTime()
    {
        return ProductService::getSaledLatelyTime($this->id);
    }

    /**
     * 获取商品最近一次售出订单信息
     * @return array
     */
    public static function getSaledLatelyOrder($id)
    {
        return Cache::tags('product_saled_lately_order')->remember($id, 360, function () use ($id) {
           return OrderService::getNewestOrderByPid($id, ['id','sn','created_at']);
        });
    }

    public function isProductLibraryPreSale()
    {
        return ProductPresell::where('product_id',$this -> id) -> select('id') -> first() ? true : false;
    }

    public function isShopPreSale()
    {
        $preSalePeriodSwitch = Settings::fetch('is_pre_sale_period');
        if($preSalePeriodSwitch)
        {
            $fromDate = Settings::fetch('from_date');
            $toDate = Settings::fetch('to_date');
            
            if(!is_null($fromDate) && !is_null($toDate))
            {
                $productLibraryPreSaleStatus = $this -> isProductLibraryPreSale();

                $fromDate = strtotime($fromDate);
                $toDate = strtotime($toDate);

                $nowDate = time();

                return $productLibraryPreSaleStatus && $nowDate <= $toDate && $nowDate >= $fromDate;
            }

            return false;
        }

        return false;
    }
    /*
     * 获取选品人
     */
    public function getPriorityUser(){
        $priorityUser = Product::where('priority_user','!=','')->groupBy('priority_user')->lists('priority_user','priority_user');
        $priorityUser = array_filter($priorityUser, function ($var) {
            return !empty($var);
        });
        return $priorityUser;
    }

    /** @noinspection DuplicatedCode */
    protected function handleFeedImageScale(string $image_thumb, string $feed_thumb = '', string $protocol = ''): string
    {
        // 在命令行刷新 ES 时，可能会没有 Request 对象，所以，这里使用默认 https://
        $protocol = $protocol ?: (string)(strstr(request()->root(), '//', true) ?: 'https:');
        // 严格地说，我要从左往右删除这几个字符，虽然这里表明了
        // 它有点儿类似 substr($url,strpos($url,':') + 1)
        $ltrimChar = 'https:';
        // 对入参进行处理，入参也会存在 协议
        $image_thumb = ltrim($image_thumb, $ltrimChar);
        $feed_thumb = ltrim($feed_thumb, $ltrimChar);
        // 如果两个图片地址都是空的，那就直接返回
        if (empty($image_thumb) && empty($feed_thumb)) {
            return $image_thumb;
        }
        try {
            // 如果当前 feed 没有图片，就抛出异常，下面会接住，然后打开缩略图去处理
            if (!$feed_thumb) {
                throw new InvalidArgumentException('Feed Thumb Not Found!');
            }
            $image = ImageManagerStatic::make($protocol . $feed_thumb);
            // 如果这个图片能打开，就用他替换掉 image_thumb ，因为后面的代码都是返回的这个变量
            $image_thumb = $feed_thumb;
        } catch (Throwable $exception) {
            try {
                $image = ImageManagerStatic::make($protocol . $image_thumb);
                // 如果这个图片打开失败，就使用入参了
            } catch (Throwable $exception) {
                return $image_thumb;
            }
        }

        $height = max($image->width(), $image->height());
        $width = min($image->width(), $image->height());

        // 如果 高度和宽度一样，说明已经是 1:1 了，不需要处理
        if ($height === $width) {
            return $image_thumb;
        }

        // 容差值
        $tolerance = 10;
        // 容差处理，如果宽高差距不是很大，就直接返回使用
        if (abs($height - $width) < $tolerance) {
            return $image_thumb;
        }

        // 创建一个 1:1 新画布，以原图片高度为基准，背景白色。
        $newImage = ImageManagerStatic::canvas($height, $height, '#ffffff');
        // 把图片插入到画布中间
        $newImage->insert($image, 'center');
        // 销毁旧图片
        $image->destroy();

        $ext = pathinfo($image_thumb, PATHINFO_EXTENSION);
        $path = $this->createTempFile($ext);
        $newImage->save($path);
        // 销毁图片
        $newImage->destroy();
        // 创建一个文件模型
        $file = app(File::class);
        $file->data = $path;
        // 生成一下保存的文件名字
        $file->disk_name = $this->generateRandomFileName('.' . $ext);
        // 保存将自动上传到 S3
        $file->save();
        // 这里生成的地址有 http[s]: ，在后面的拼接中不需要这个。
        return ltrim($file->getPath(), $ltrimChar);
    }

    /**
     * 创建一个临时文件，并返回路径。
     *
     * @return false|string
     * @throws \Exception
     */
    protected function createTempFile(string $ext = 'png')
    {
        // 创建临时目录
        $path = sys_get_temp_dir();
        $path .= vsprintf('/feed_thumb_temp_%s_%d.%s', [
            str_replace('.', '', uniqid('', true)),
            random_int(1111, 9999),
            $ext,
        ]);
        $path = str_replace('\\', '/', $path);
        return $path;
    }

    /**
     * @param string $ext 文件名后缀
     * @param int $limit 名字限制长度
     * @return string
     */
    protected function generateRandomFileName(string $ext,$limit = 22): string
    {
        return substr(md5(microtime().mt_rand()), 0, $limit) . $ext;
    }

    /**
     * @param string $feed_thumb
     *
     * @param string $f_thumb
     *
     * @return string
     */
    private function getFacebookFeedThumb(string $feed_thumb, string $f_thumb = ''): string
    {
        // 如果 feed_thumb 有值就用它，如果没有就用首图
        $feed_thumb = $feed_thumb ?: $f_thumb;
        if (empty($feed_thumb)) {
            return '';
        }
        // 是否启用 Facebook feed ，如果没有就返回原来 feed
        if (!Settings::fetch('enabled_facebook_thumb_feed')) {
            return $feed_thumb;
        }
        try {
            /**
             * 启用 OSS 的站点额外处理
             */
            if (MoresiteService::isEnabledOSS()) {
                return $this->handleFeedImageScaleByAliOss($feed_thumb);
            }

            // 进行图片处理
            return $this->handleFeedImageScale($f_thumb, $feed_thumb);
        } catch (Throwable $exception) {
            // 这个是原来的，也就是获取失败时就要走这里
            return $feed_thumb;
        }
    }

    /**
     * es 获取特色图
     */

    public static function getEsPopFeatureImage(array $ids){
        if (empty($ids))return [];
        $es_query = [
            'query' => [
                'bool' => [
                    'must' => [
                        ['terms'=>['id' => $ids]]
                    ]
                ]
            ],
            'size'=>9999,
            '_source' => ['id','feature_image.path'],
        ];
        $options = ['type' => 'products'];
        $data  = Searches::searchDataList($es_query, $options);
        $data  = isset($data['hits']['hits'])?array_column($data['hits']['hits'],'_source'):[];
        $data  = array_column($data,'feature_image','id');
        $data = array_map(function ($value){
            return array_column($value,'path');
        },$data);
        return $data;
    }

    public function getMetaDescriptionAttribute($value){
        if (!$value){
            return $this->name2;
        }
        return $value;
    }
    
    private function getFacebookFeedThumbs(array $images): array
    {
        $result = [];
        $images = array_filter($images);

        foreach ($images as $image){
            $url = $this->getFacebookFeedThumb($image);
            
            if($url){
                $result[] = $url;
            }
        }
        return $result;
    }

    public static function clearCache($productId)
    {
        $sceneId =  SceneService::contextSceneId();
        // 评论缓存更新
        Review::clearCache($productId, $sceneId);

        $tag = self::getCacheTag();
        $key = json_encode(['id'=>$productId]);
        \Cache::tags($tag)->forget($productId);
        \Cache::tags($tag)->forget($key);

        self::clearPriceCache($productId);

        \Cache::tags('product-sku-switch')->forget($productId);
        $userGroups = UserGroup::lists('id');
        foreach ($userGroups as $group) {
            $groupKey = $productId . '-' . $group;
            \Cache::tags('product-sku-switch')->forget($groupKey);
        }
        Cache::tags('product-relate:' . $productId)->flush();
    }

    /**
     * 清理产品价格缓存 getDbPrice getSrcPrice2 getDbListPrice getSrcPrice ... 等
     * @param int|array $pids 产品IDS
     * @return void
     */
    public static function clearPriceCache($pids)
    {   
        $sceneId =  SceneService::contextSceneId();
        if(!is_array($pids)){
            $pids = [$pids];
        }
        foreach($pids as $pid){
            \Cache::tags('products.dbProductInfo')->forget($pid . '-' . $sceneId);
            \Cache::tags('products.srcprice2')->forget($pid);
        }
    }

    public static function getCacheTag()
    {
        $local = Translator::instance()->getLocale();

        return 'products:'.$local;
    }
    public static function getStockPiles(){
        return [
            'all_stock_up'=>1,
            'part_stock_up' => 2,
            'confirmed_in_stock' => 3,
            'suspected_in_stock' => 4
        ];
    }

    /**
     * 获取sku不同用户级别缓存key
     * @param $productId
     * @return string
     */
    public static function getSkuCacheKey($productId)
    {
        $user = (new Account)->user();
        $cacheKey = $productId;
        if ($user) {
            $userGroups = $user->groups->lists('id');
            $cacheKey .= ':'.md5(json_encode($userGroups));
        }
        return $cacheKey;
    }


    //获取spu囤货状态
    public function getSpuStockpile($skus = []){
        $stockPileStatus = array_column($skus, 'stockpile_status');
        $stockPiles = self::getStockPiles();

        $result = [];
        //囤货状态
        if(in_array(1,$stockPileStatus) && !in_array(0,$stockPileStatus)){
            //囤货
            $result[] = $stockPiles['all_stock_up'];
        }elseif(in_array(1,$stockPileStatus) && in_array(0,$stockPileStatus)){
            //部分囤货
            $result[] = $stockPiles['part_stock_up'];
        }

        if(!empty($result)){
            return $result;
        }
        //确认有货状态
        $storeStatus = array_column($skus, 'store_status');
        
        if(in_array(1,$storeStatus)){
            //确认有货
            $result[] = $stockPiles['confirmed_in_stock'];
        }
        if(in_array(0,$storeStatus)){
            //疑似有货
            $result[] = $stockPiles['suspected_in_stock'];
        }
        $result = $result ?: [$stockPiles['suspected_in_stock']];
        return $result;
    }    

    public function getTagIconIdOptions()
    {
        $options = TagIcon::select('id','name')->lists('name','id');
        return $options;
    }
    
    public function getTags()
    {
        return Tag::lists('name','id');
    }

    public function scopeInPromotionsName($query,$promo_id)
    {
        $productIdArr = [];
        $promoProductIdArrs = PromoCondition::whereIn('promo_id',$promo_id)
            -> where('type','products')
            -> lists('values');
        foreach($promoProductIdArrs as $idVal){
            $idArr = json_decode($idVal,true);
            if(!is_array($idArr)){
                continue;
            }
            $productIdArr = array_merge($productIdArr, $idArr);
        }
        return $query->whereIn('id',$productIdArr);
    }
    /**
     * 组装场景数据
     * @return array
     */
    public function packageSceneData()
    {   
        $sceneList = SceneService::all();

        $items = [];
        foreach ($sceneList as $scene) {
            $data = $this->scene_products->first(function ($key, $item) use ($scene) {
               return $item->scene_id == $scene['id'];
            });

            if (!$data) {
                continue;
            }
            $data = $data->toArray();

            //设置当前场景ID
            SceneService::setTempSceneId($data['scene_id'] ?? 0);

            $data = $this->getScenePrice($data);
            $data['reviews_count'] = Review::getSceneReviewsCount($data['product_id']);
            SceneService::clearTempSceneId();
            //清除当前场景ID

            // 特征图信息
            // 缩略图信息
            // 产品特征
            $relations = ['feature_image', 'thumb', 'features'];
            foreach ($relations as $relationName) {
                $data[$relationName] = $this->filterSceneData($relationName, $data['scene_id']);
            }
            $data = Event::fire('jason.ccshop.getThumb', [$data, 'products', MoresiteService::getMainDomain()], true);
            $fThumb = '';
            if (!empty($data['feature_image'])) {
                $fThumb = data_get(current($data['feature_image']), 'path');
            } elseif (!empty($data['thumb'])) {
                $fThumb = data_get(current($data['thumb']), 'path');
            }

            $data['f_thumb'] = $fThumb;
            $items[] = $data;
        }
        return $items;
    }
    /**
     * 获取商品场景价格
     */
    private function getScenePrice($data){
        $default_price        = $this->price;
        $default_list_price   = $this->list_price;
        $default_src_price    = $this->src_price;
        //多场景商品价格计算
        //商品价格设置为多场景价格，再计算
        $this->price = $data['price'];
        $this->src_price = $data['price'];
        $this->list_price = $data['list_price'];
        $labels = [];
        
        $sellingPrice = Promotions::productSellingPrice($this,'detail',$labels);
        $src_price             = (string)$sellingPrice['src_price'];
        $promotion_price       = (string)$sellingPrice['price'];
        if(empty(Settings::fetch('is_down_usd_price_ceil','0'))){  //美元站是否开启取整
            $src_price       = ceil($src_price);
            $promotion_price = ceil($promotion_price);
        }
        $data['src_price']       = $src_price;
        $data['promotion_price'] = $promotion_price;
        $data['list_price']       = ceil((string)$sellingPrice['list_price']);
        if ($data['promotion_price'] < $data['list_price']) {
            $data['price'] = $data['promotion_price'];
        }
        $data['discount'] = $this->getDiscount($data['list_price'],$data['price'],$data['promotion_price']);
        
        $this->price = $default_price;
        $this->src_price =  $default_src_price;
        $this->list_price = $default_list_price;
        return $data;
    }
    /**
     * 获取商品场景数据
     */
    public function getSceneData($scene_id=0){
        $data = $this->scene_products->first(function ($key, $item) use ($scene_id) {
            return $item->scene_id == $scene_id;
        });
        if (!$data) {
            return $this->toArray();
        }
        $data = $data->toArray();
        SceneService::setTempSceneId($data['scene_id'] ?? 0);
        $data = $this->getScenePrice($data);
        SceneService::clearTempSceneId();
        return $data;
    }
    /**
     * 过滤场景数据
     * @param $relationName
     * @param $sceneId
     * @return mixed
     */
    private function filterSceneData($relationName, $sceneId)
    {
        $items = $this->filterRelationProductData($relationName, $sceneId);
        if (!$items) {
            $items = $this->filterRelationProductData($relationName);
        }
        if ($relationName == 'features'){
            return Features::formatFeatures($items, ['scene_id' => $sceneId]);
        }

        return $items->toArray();
    }

    /**
     * 过滤关联场景模型数据
     * @param $relationName
     * @param int $sceneId
     * @return \Illuminate\Support\Collection
     */
    private function filterRelationProductData($relationName, $sceneId = 0)
    {
        $items = $this->{$relationName}->filter(function ($item) use($sceneId) {
            return $item->scene_id == $sceneId;
        });

        $items = $items ?: collect([]);

        return $items->values(); // 调取values目的防止解决filter后, key可能不是从0开始, 会导致后续通过下标获取异常
    }

    private function handleFeedImageScaleByAliOss(string $feed_thumb): string
    {
        if (empty($feed_thumb)) {
            return '';
        }

        $src = strstr($feed_thumb, '?', true) ?: $feed_thumb;
        $src .= '?x-oss-process=' . urlencode('image/resize,l_602,m_pad,color_FFFFFF');

        return $src;
    }

    /**
     * Notes:获取特定时间范围内的运营上新数量
     * User: ma
     * Date: 2021/5/7
     * Time:15:01
     */
    public static function getInstockNum($startTime,$endTime)
    {

        $data = Product::select('admin_id', \Illuminate\Support\Facades\DB::raw('GROUP_CONCAT(id) as pids,count(id) as total'))
        ->whereBetween('instock_time', [$startTime, $endTime])
        ->groupBy('admin_id')
        ->get()->toArray();
        return array_column($data,null,'admin_id');
    }

    //关联产品,订单,订单商品表
    public static function joinOrderProducts($query,$startTime, $endTime)
    {
        return $query->leftJoin('cc_orders', 'cc_orders.id', '=', 'cc_order_products.order_id')
                    ->leftJoin('cc_products','cc_products.id', '=', 'cc_order_products.product_id')
                    ->whereBetween('cc_products.instock_time', [$startTime, $endTime])
                    ->whereBetween('cc_orders.created_at', [$startTime, $endTime])
                    ->whereNull('cc_orders.deleted_at')
                    ->whereIn('cc_orders.status_id',OrderStatus::saledStatusIds());
    }

    /**
     * Notes:获取特定时间范围内出单款数
     * User: ma
     * Date: 2021/5/7
     * Time:15:01
     */
    public static function getSaleSpuNum($startTime,$endTime)
    {
        $query = DB::table('cc_order_products')->select('cc_products.admin_id',\Illuminate\Support\Facades\DB::raw('GROUP_CONCAT(DISTINCT cc_products.id) as pids,count(DISTINCT cc_products.id) as total'));
        $data = self::joinOrderProducts($query,$startTime,$endTime)
            ->groupBy('cc_products.admin_id')
            ->get();
        return array_column($data,null,'admin_id');
    }

    /**
     * Notes:获取特定时间范围内出单件数数
     * User: ma
     * Date: 2021/5/7
     * Time:15:01
     */
    public static function getSaleSkuNum($startTime,$endTime)
    {
        $query = DB::table('cc_order_products')->select('cc_products.admin_id',\Illuminate\Support\Facades\DB::raw('sum(cc_order_products.qty) as total'));
        $data = self::joinOrderProducts($query,$startTime,$endTime)
            ->groupBy('cc_products.admin_id')
            ->get();
        return array_column($data,null,'admin_id');
    }

    /**
     * Notes:获取特定时间范围爆款数量
     * User: ma
     * Date: 2021/5/7
     * Time:15:01
     */
    public static function getHotSaleSkuNum($startTime,$endTime,$diffDays,$hotNum)
    {
//        DB::connection()->enableQueryLog();
        $query = DB::table('cc_order_products')->select('cc_products.admin_id','cc_products.id');
        $data = self::joinOrderProducts($query,$startTime,$endTime)
            ->groupBy('cc_products.id')
            ->havingRaw("sum(cc_order_products.qty) >= ?",[$diffDays*$hotNum])
            ->get();
//        print_r(DB::getQueryLog());die;
        $newData = [];
        foreach ($data as $item) {
            if (isset($newData[$item->admin_id])) {
                $newData[$item->admin_id]['total'] += 1;
                $newData[$item->admin_id]['pids'] .= ",".$item->id;
            } else {
                $newData[$item->admin_id]['total'] = 1;
                $newData[$item->admin_id]['pids'] = $item->id;
            }
        }
        return $newData;
    }

    /**
     * Notes:获取置顶字段的修改数量
     * User: ma
     * Date: 2021/5/10
     * Time:14:24
     * @param $startTime
     * @param $endTime
     * @param $field
     * @param $sceneID
     * @return array
     */
    public static function getFieldEditNum($startTime,$endTime,$field,$sceneID)
    {
        if ($sceneID) {
            $data = SystemRevisions::select('system_revisions.user_id as admin_id',\Illuminate\Support\Facades\DB::raw('GROUP_CONCAT(DISTINCT cc_product_scene.product_id) as pids,count(DISTINCT cc_product_scene.product_id) as total'))
                ->leftJoin('cc_product_scene','cc_product_scene.id', '=', 'system_revisions.revisionable_id')
                ->whereBetween('system_revisions.created_at', [$startTime, $endTime])
                ->where('system_revisions.revisionable_type', 'Jason\Ccshop\Models\ProductScene')
                ->where('system_revisions.field',$field)
                ->where('cc_product_scene.scene_id',$sceneID)
                ->groupBy('admin_id')
                ->get()->toArray();
        } else {
            $data = SystemRevisions::select('user_id as admin_id',\Illuminate\Support\Facades\DB::raw('GROUP_CONCAT(DISTINCT revisionable_id) as pids,count(DISTINCT revisionable_id) as total'))
                ->whereBetween('created_at', [$startTime, $endTime])
                ->where('revisionable_type', 'Jason\Ccshop\Models\Product')
                ->where('field',$field)
                ->groupBy('admin_id')
                ->get()->toArray();
        }
        
        return array_column($data,null,'admin_id');
    }

    /**
     * Notes:根据在库时间和销量范围获取商品列表
     */
    public static function getSaledNumByInstockAndRange($startTime,$endTime,$rangeStart,$rangeEnd,$pageSize = 60,$orders = [],$page = 1)
    {
        $saleSatus = OrderStatus::saledStatusIds();

            $query = DB::table('cc_products')->select('cc_products.id','cc_products.name',DB::raw('ifnull(sum(cc_order_products.qty),0) as total'))
            ->leftJoin('cc_order_products','cc_products.id', '=', 'cc_order_products.product_id')
            ->leftJoin('cc_orders', 'cc_orders.id', '=', 'cc_order_products.order_id')
            ->whereBetween('cc_products.instock_time', [$startTime, $endTime])
            ->whereNull('cc_orders.deleted_at')
            ->where('cc_products.status','instock')
            ->where(function($query) use ($saleSatus){
                $query->whereIn('cc_orders.status_id',$saleSatus)
                    ->orWhereNull('cc_orders.status_id');
            })
            ->groupBy('cc_products.id');

        if ($rangeStart) {
            $query->having('total', '>=', $rangeStart);
        }
        if ($rangeEnd) {
            $query->having('total', '<=', $rangeEnd);
        }

        if (empty($orders)) {
            $orders = ['total' => 'desc'];
        }

        if (is_array($orders) && !empty($orders)) {
            foreach ($orders as $field => $sort) {
                $query = $query->orderBy($field, $sort);
            }
        }
        $count = count($query->get());
        $offset = ($page - 1) > 0 ? $page * $pageSize : 0;
        $data = $query->skip($offset)->take($pageSize)->get();
        return ['total' => $count, 'data' => $data];
    }
    //新增获取产品标识
    public function getLabelsList()
    {
        $user = BackendAuth::getUser();
        if (!$user->hasAccess('jason.ccshop.labels')) {
            return [];
        }
        return Label::where('is_delete',0)->orderBy('id','desc')->limit('500')->lists('name','id');
    }
    //产品标识筛选
    public function scopeProductLabel($query, $ids)
    {
        return $query->join('cc_label_to_products','cc_label_to_products.product_id','=','cc_products.id')->groupBy('cc_label_to_products.product_id')->whereIn('cc_label_to_products.label_id',$ids);
    }

    /**
     * 根据提供的sku 获取原始的skuID(采集时平台站点的SKU)
     */
    public function getOriginSkuId($sku)
    {
        $products = ProductService::getProductsByIdsFromCache([$this->id]);
        $product = array_pop($products);
        if (!empty($product['sku'])) {
            $skuInfo = array_first($product['sku'], function ($key, $val) use ($sku) {
                return $val['sku'] == $sku;
            }, []);

            return data_get($skuInfo, 'item_id');
        }

        return ProductSku::where('product_id', $this->id)->where('sku', $sku)->pluck('item_id');
    }
    /**
     * 链式关联商品筛选
     */
    public function scopeFilterChainProducts($query){
        $mainPid = App::make('Jason\Ccshop\Controllers\Products')->getCurrentPidByBackendUrl();
        $mainProduct = DB::table('cc_products')->where('id',$mainPid)->select('id','admin_id')->first();
        //获取相同选品人的商品
        if($mainProduct){
            $query->where('admin_id',$mainProduct->admin_id);
        }
        $query->leftJoin('cc_product_chain_related','cc_products.id','=','cc_product_chain_related.related_id')->whereNull('cc_product_chain_related.related_id');
        return $query;
    }

    /**
     * 获取根据分类分组的 上新数量
     * @param $startTime
     * @param $endTime
     * @param $adminIds
     * @param $category_ids
     * @return array
     */
    public static function getInstockNumForCategory($startTime,$endTime,$adminIds,$category_ids)
    {

        $query = DB::table('cc_products as p')->select('p_c.category_id', \DB::raw('count(DISTINCT id) as total'))
            ->leftJoin('cc_product_categories as p_c', 'p_c.product_id', '=', 'p.id')
            ->whereBetween('p.instock_time', [$startTime, $endTime])
            ->whereIn('p_c.category_id', $category_ids);
        if(!empty($adminIds)){
            $query->whereIn('p.admin_id',$adminIds);
        }
        $data =  $query->groupBy('p_c.category_id')->get();
        return array_column($data,null,'category_id');
    }

    /**
     * Notes:获取根据分类分组的 统计数据
     * @param $product_start_time
     * @param $product_end_time
     * @param $order_start_time
     * @param $order_end_time
     * @param $domain
     * @param $admin_ids
     * @param $category_ids
     * @param $type
     * @return array
     */
    public static function getSaleStatisticalDataForCategory($product_start_time,$product_end_time,$order_start_time,$order_end_time,$domain,$admin_ids,$category_ids,$type='spu')
    {
        $select[] = 'cc_product_categories.category_id';
        switch ($type){
            case 'spu': //成交款数
                $select[] = \DB::raw('count(DISTINCT cc_products.id) as total');
                break;
            case 'sku'://成交件数
                $select[] = \DB::raw('sum(cc_order_products.qty) as total');
                break;
            case 'total_price'://成交总金额
                $select[] = \DB::raw('sum(cc_order_products.price * cc_order_products.qty) as total');
                break;
        }

        $query = DB::table('cc_order_products')->select($select);
        $data = self::joinOrderProductsForCategory($query,$product_start_time,$product_end_time,$order_start_time,$order_end_time,$domain,$admin_ids,$category_ids)
            ->groupBy('cc_product_categories.category_id')
            ->get();
        return array_column($data,null,'category_id');
    }


    /**
     * 关联产品,订单,订单商品表,产品分类
     * @param $query
     * @param $product_start_time
     * @param $product_end_time
     * @param $order_start_time
     * @param $order_end_time
     * @param $domain
     * @param $adminIds
     * @param $category_ids
     * @return mixed
     */
    public static function joinOrderProductsForCategory($query,$product_start_time,$product_end_time,$order_start_time,$order_end_time,$domain,$adminIds,$category_ids)
    {
        //销量分析创建时间使用在库时间
        $timeType = Settings::fetch('order_sale_data_time_type', 0);
        $sql_timeType = $timeType ? 'cc_products.instock_time' : 'cc_products.created_at';

         $query->leftJoin('cc_orders', 'cc_orders.id', '=', 'cc_order_products.order_id')
            ->leftJoin('cc_products','cc_products.id', '=', 'cc_order_products.product_id')
            ->leftJoin('cc_product_categories','cc_products.id', '=', 'cc_product_categories.product_id')
            ->whereBetween($sql_timeType, [$product_start_time, $product_end_time])
            ->whereBetween('cc_orders.created_at', [$order_start_time, $order_end_time])
            ->whereIn('cc_product_categories.category_id', $category_ids)
            ->where('cc_orders.firstname','not like','测试%')
            ->where('cc_orders.lastname','not like','测试%')
            ->whereNull('cc_orders.deleted_at')
            ->whereIn('cc_orders.status_id',OrderStatus::saledStatusIds());
        if(!empty($domain)){
            $query->where('cc_orders.domain',$domain);
        }
        if(!empty($adminIds)){
            $query->whereIn('cc_products.admin_id',$adminIds);
        }
        return $query;
    }

    //获取店铺
    public function getBusinessList()
    {
        $user = BackendAuth::getUser();
        if (!$user->hasAccess('jason.ccshop.business')) {
            return [];
        }
        return Business::query()->orderBy('id','desc')->lists('name','id');
    }
    //店铺筛选
    public function scopeBusiness($query, $ids)
    {
        return $query->whereHas('business', function($q) use ($ids) {
            $q->whereIn('id',$ids);
        });
    }

    //获取店铺组
    public function getBusinessGroupList()
    {
        return BusinessGroup::query()->orderBy('id','desc')->lists('name','id');
    }
    //店铺组筛选
    public function scopeBusinessGroup($query, $ids)
    {
        $business_ids = DB::table('cc_business_group_relation')->whereIn('business_group_id', $ids)->lists('business_id');
        return $query->whereHas('business', function($q) use ($business_ids) {
            $q->whereIn('id',$business_ids);
        });
    }

    /**
     * 格式化价格
     * @param $price
     * @param $code
     * @return false|float|int|string
     */
    public function displayPrice($price,$code=null)
    {
        if (empty($options)) {
            $options = ['symbol' => true, 'rate' => true, 'format' => true];
        }
        return ShopBase::formatPrice($price, $code, $options);
    }

    public function scopeSpecialPriceStatus($query, $status)
    {
        if (empty($status) || count($status) > 1) {
            return;
        }

        if ($status[0] === 'yes') {
            $query->where('special_price', '>', 0);
        } else {
            $query->where('special_price', 0);
        }
    }

    /**
     * 当前站点域名
     * @return mixed|string
     */
    public static function getDomain()
    {
        $res = explode('.',$_SERVER['HTTP_HOST']);
        $index = count($res) - 2;
        return $res[$index];
    }
}
