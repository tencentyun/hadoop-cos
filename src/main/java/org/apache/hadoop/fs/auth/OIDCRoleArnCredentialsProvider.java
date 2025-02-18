package org.apache.hadoop.fs.auth;

import com.qcloud.cos.auth.BasicSessionCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.auth.COSCredentialsProvider;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.provider.OIDCRoleArnProvider;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

public class OIDCRoleArnCredentialsProvider extends AbstractCOSCredentialProvider implements COSCredentialsProvider {
	private static final Logger log = LoggerFactory.getLogger(OIDCRoleArnCredentialsProvider.class);

	private OIDCRoleArnProvider provider;
	private boolean initialized = false;
	private AtomicReference<Credential> lastCredentialsRef;

	public OIDCRoleArnCredentialsProvider(@Nullable URI uri, Configuration conf) {
		super(uri, conf);
		try {
			this.provider = new OIDCRoleArnProvider();
			lastCredentialsRef = new AtomicReference<>();
			initialized = true;
		} catch (Exception e) {
			log.error("Failed to initialize OIDC Role Arn Credentials Provider", e);
		}
	}

	@Override
	public COSCredentials getCredentials() {
		if (!initialized) {
			return null;
		}
		COSCredentials cosCredentials = null;
		try {
			Credential cred;
			// TODO: 这里下游有个bug，provider.getCredentials()调用两次，第二次返回的是null
			//  下游获取逻辑是先初始化空的credentials，然后调用update方法
			//  update方法里会判断上一次获取credentials的时间与当前时间差是否大于阈值，只有大于阈值时才会更新
			//  因此这里我们缓存住了第一个credentials，通过refresh去更新这个credentials
			if (lastCredentialsRef.get() != null) {
				cred = lastCredentialsRef.get();
				provider.update(cred);
			} else {
				cred = this.provider.getCredentials();
			}
			lastCredentialsRef.set(cred);
			cosCredentials = new BasicSessionCredentials(cred.getSecretId(), cred.getSecretKey(),
					cred.getToken());
		} catch (Exception e) {
			log.error("Failed to get credentials from OIDC Role Arn Credentials Provider", e);
			return null;
		}
		return cosCredentials;
	}

	@Override
	public void refresh() {
		if (!initialized) {
			return;
		}
		try {
			this.provider.update(lastCredentialsRef.get());
		} catch (Exception e) {
			log.error("Failed to refresh OIDC Role Arn Credentials Provider", e);
		}
	}
}
