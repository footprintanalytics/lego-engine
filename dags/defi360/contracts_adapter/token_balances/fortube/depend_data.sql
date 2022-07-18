SELECT distinct token_address_list[offset(0)] as token_address, pool_id, chain,
case when chain = 'BSC' then '0xc78248d676debb4597e88071d3d889eca70e5469' else '0x936e6490ed786fd0e0f0c1b1e4e1540b9d41f9ef' end  as contract_address
FROM `footprint-test-341610.struct_data.fortube_pool_info`
where protocol_slug = 'fortube'